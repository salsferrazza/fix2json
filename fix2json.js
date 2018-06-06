#! /usr/bin/env node

const fs = require('fs');
const zlib = require('zlib');
const util = require('util');
const xpath = require('xpath');
const _ = require('underscore');
const DOMParser = require('xmldom').DOMParser;
const readline = require('readline');
const StringDecoder = require('string_decoder').StringDecoder;
const YAML = require('yamljs');
const decoder = new StringDecoder();
const delim = String.fromCharCode(01); // ASCII start-of-header
let pretty = false;
let dictname;
let filename;
let TAGS = {};
let GROUPS = {};
let FIX_VER = undefined;
let rd = {};
let yaml = false;
const NUMERIC_TYPES = ['FLOAT', 'AMT', 'PRICE', 'QTY', 'INT', 'SEQNUM', 'NUMINGROUP', 'LENGTH', 'PRICEOFFSET'];

// some of these TODO's below are very speculative:
//
// TODO: decouple logic from file ingestion, but if this was a browser module, how would we package the dictionaries?
// TODO: get dictionary management out of this module
// TODO: XML merge for customizing data dictionaries with fragments
// TODO: ability to hold multiple dictionaries in memory
// TODO: autodetect FIX version from source data?
// TODO: option to flatten groups?
// TODO: emit pre and post processing events for each message processed
// TODO: forward engineer JSON to FIX?  Would be pretty useful for browser UI's based on FIX dictionaries

checkParams();

try {

    const dom = readDataDictionary(dictname);

    let input = undefined;
    if (filename) {
        if (filename.substring(filename.length - 3).toLowerCase() === '.gz') {
            input = fs.createReadStream(filename)
                .pipe(zlib.createGunzip());
        } else {
            input = fs.createReadStream(filename);
        }
    } else {
		    input = process.stdin;
    }
	  
    rd = readline.createInterface({
        input: input,
        output: process.stdout,
        terminal: false
    });

    rd.on('line', function(line) {
        if (line.indexOf(delim) > -1) {
            const msg = decoder.write(processLine(line, dom));
            console.log(msg);
        }
    });

} catch (mainException) {
    console.error("Error in main routine: " + mainException);
    process.exit(1);
}

function getMessageDefinition(messageType, dom) {
    let def = {};
    const select = '//fix/messages/message[@msgtype=\'' + messageType + '\']';
    let msg = xpath.select(select, dom);
    return msg;
}

function pluckGroup(tagArray, msgDef, groupName, validFields) {

	  let groupAnchor;
    let group = [];
    let member = {};
    let firstProp = undefined;
    let idx = 0;
    
    if (tagArray && tagArray.length > 0) {
        groupAnchor = tagArray[0].tag;
    }

    while (tagArray.length > 0) {

		    const tag = tagArray.shift();
        let key = tag.tag;
        let val = tag.val;
        let num = tag.num;
        let raw = tag.raw
        
        //        console.log(util.inspect(groupName + ': ' + GROUPS[groupName]));
        
        const tagInGroup = _.contains(validFields, key);
		    let type;
		    
		    if (TAGS[raw]) {
	    	    type = TAGS[num].type ? TAGS[num].type : 'STRING';
		    } else {
			      type = 'STRING';
		    }
		    
        if (idx > 0 && key === groupAnchor) { // add current member to group, reset member
            group.push(_.clone(member));
            member = {};
            member[key] = val;
        } else if (type === 'NUMINGROUP') { // recurse into nested repeating group
            member[key] = val;
            if (val > 0) {
                const newGroup = pluckGroup(tagArray, msgDef, key, validFields);
                member[key.substring('No'.length)] = newGroup.group;
                tagArray = newGroup.fieldsLeft;
            }
        } else if (!tagInGroup) { // we've reached the end of the group
            group.push(_.clone(member)); // add the last processed member to the group
            tagArray.unshift(tag); // put this guy back, he doens't belong here
            return { group: group, fieldsLeft: tagArray };
        } else {
            member[key] = val; // tag is a member of an in-flight group
        }
		    
        idx++;

	  }

    return { group: {}, fieldsLeft: tagArray };
    
}

function getMessageFields(msgDef, dom) {
    let fields = [];

    const nodes = msgDef[0].childNodes;

    for (let i = 0; i < nodes.length; i++) {
        let type;
        let name;
        type = nodes[i].nodeName;

        if (type !== '#text') {
            if (nodes[i].attributes) {
                name = nodes[i].attributes[0].value;
            } else if (nodes[i].nodeName) {
                name = nodes[i].nodeName;
            }
            if (type === 'field') {
                fields.push(name);
            } else if (type === 'component') {
                fields = fields.concat(flattenComponent(name, dom));
            }
        }
    }

    //    console.log(msgDef[0].attributes[0].value + ': ' + _.uniq(fields).length);
    return fields;

}

function resolveFields(fieldArray, dom) {

    targetObj = {};
    let group = [];

    // 35 is tag num for msgtype
    const msgType = _.findWhere(fieldArray, { num: '35' }); 
    let msgDef = getMessageDefinition(msgType.raw, dom);
    let validFields = getMessageFields(msgDef, dom);

    while (fieldArray.length > 0) {

        const field = fieldArray.shift();
        const key = field.tag;
        const val = field.val;
        const raw = field.raw;
        const num = field.num;
        let type = undefined;
        
        if (TAGS[num]) {
	    	    type = TAGS[num].type ? TAGS[num].type : 'STRING';
		    } else {
			      type = 'STRING';
		    }

        if (type === 'NUMINGROUP') {
            let newGroup = pluckGroup(fieldArray, msgDef, key, validFields);
            targetObj[key] = val;
            targetObj[key.substring('No'.length)] = newGroup.group;
            fieldArray = newGroup.fieldsLeft;
        } else {
            targetObj[key] = val;
        }
    }
    return targetObj;
}

function processLine(line, dom) {
    let targetObj = resolveFields(extractFields(line), dom);
    if (yaml) {
        return YAML.stringify(targetObj, 256);
    } else {
        return pretty ? JSON.stringify(targetObj, undefined, 2) : JSON.stringify(targetObj)
    }
}

function extractFields(record) {

    let fieldArray = [];
    let fields = record.split(delim);
    for (let i = 0; i < fields.length; i++) {
        let both = fields[i].split('=');
        both[0].replace("\n", '').replace("\r", '');
        if (both[1] !== undefined && both[0] !== undefined) {
            let val = both[1];
            if (TAGS[both[0]] && TAGS[both[0]].type) {
                val = _.contains(NUMERIC_TYPES, TAGS[both[0]].type) ? Number(val) : val;
            }
            val = mnemonify(both[0], val);
            fieldArray.push({
                tag: TAGS[both[0]] ? TAGS[both[0]].name : both[0],
                val: val,
                num: both[0],
                raw: both[1]
            });
        }
    }
    return fieldArray;
}

function mnemonify(tag, val) {
    return TAGS[tag] ? (TAGS[tag].values ? (TAGS[tag].values[val] ? TAGS[tag].values[val] : val) : val) : val;
}

function flattenComponent(componentName, dom) {
    let fieldNames = [];
    const components = xpath.select('//fix/components/component', dom);

    if (!components || components.length === 0) {
        console.error('could not find component: ' + componentName);
        return fieldNames;
    } else {
        for (let i = 0; i < components.length; i++) {
            const fields = components[i].getElementsByTagName('field');
            for (let j = 0; j < fields.length; j++) {
                fieldNames.push(fields[j].attributes[0].value);
            }
            const comps = components[i].getElementsByTagName('component');
            for (let k = 0; k < comps.length; k++) {
                const compName = comps[k].attributes[0].value;
            }
        }
        return _.uniq(fieldNames);
    }
}


function getFixVer(dom) {
    const fixMaj = xpath.select("//fix/@major", dom)[0].value;
    const fixMin = xpath.select("//fix/@minor", dom)[0].value;
    const fixSp = xpath.select("//fix/@servicepack", dom)[0].value;
    FIX_VER = [fixMaj, fixMin, fixSp].join('.');
}

function readDataDictionary(fileLocation) {

    // TODO: lazy load data dictionary based upon message type coming in
    
    const xml = fs.readFileSync(fileLocation).toString();
    const dom = new DOMParser().parseFromString(xml);
    const nodes = xpath.select("//fix/fields/field", dom);

    getFixVer(dom);

    // messages have fields, components and groups (4.x?)
    // fields have tags
    // components have fields and groups
    // find all components
    // list their direct child fields 
    // figure out which component a particular group field belongs to
    // if in nested repeating group, must know context of current group name
    // just knowing all the possible fields under a particular message is not smart enough
    
    
    for (let i = 0; i < nodes.length; i++) {
        const tagNumber = nodes[i].attributes[0].value;
        const tagName = nodes[i].attributes[1].value;
        const tagType = nodes[i].attributes[2].value;
        const valElem = nodes[i].getElementsByTagName('value');
        let values = {};
        for (let j = 0; j < valElem.length; j++) {
            values[valElem[j].attributes[0].value] = valElem[j].attributes[1].value.replace(/_/g, ' ');
        }
        TAGS[tagNumber] = {
            name: tagName,
            type: tagType,
            values: values
        };
    }

    return dom;
    
}

function checkParams() {

    if (process.argv.length < 3) {
        console.error("Usage: fix2json [-p] <data dictionary xml file> [path to FIX message file]");
        console.error("\nfix2json will use standard input in the absence of a message file.");
        process.exit(1);
    } else if (process.argv.length === 3) {
        dictname = process.argv[2];
    } else if (process.argv.length === 4) {
        if (process.argv[2] === '-p') {
            pretty = true;
            dictname = process.argv[3];
        } else {
            dictname = process.argv[2];
            filename = process.argv[3];
        }
    } else if (process.argv.length === 5) {
        pretty = true;
        dictname = process.argv[3];
        filename = process.argv[4];
    }
    if (process.argv[1].indexOf('yaml') > 0) {
        yaml = true;
    }

}
