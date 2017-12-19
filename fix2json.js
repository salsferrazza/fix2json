#! /usr/bin/env node

var fs = require('fs');
var zlib = require('zlib');
var util = require('util');
var xpath = require('xpath');
var _ = require('underscore');
var DOMParser = require('xmldom').DOMParser;
var readline = require('readline');
var StringDecoder = require('string_decoder').StringDecoder;
var YAML = require('yamljs');
var decoder = new StringDecoder();
var pretty = false;
var dictname;
var filename;
var TAGS = {};
var GROUPS = {};
var MESSAGES = {};
var FIX_VER = undefined;
var rd = {};
var yaml = false;
var NUMERIC_TYPES = ['FLOAT', 'AMT', 'PRICE', 'QTY', 'INT', 'SEQNUM', 'NUMINGROUP', 'LENGTH', 'PRICEOFFSET'];

// some of these TODO's below are very speculative:
//
// TODO: decouple logic from file ingestion, but if this was a browser module, how would we package the dictionaries?
// TODO: get dictionary management out of this module
// TODO: autodetect FIX version from source data?
// TODO: emit pre and post processing events for each message processed
// TODO: forward engineer JSON to FIX?  Would be pretty useful for browser UI's based on FIX dictionaries

const delim = String.fromCharCode(01); // ASCII start-of-header
let options = checkParams();

try {

    let dict = readDataDictionary(options.dict);

//    console.log(util.inspect(dict.fields, undefined, null));
    var input;
    if (options.file) {
        if (options.file.substring(filename.length - 3).toLowerCase() === '.gz') {
            input = fs.createReadStream(options.file)
                .pipe(zlib.createGunzip());
        } else {
            input = fs.createReadStream(options.file);
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
            var msg = decoder.write(processLine(line, options, dict));
            console.log(msg);
        }
    });

} catch (mainException) {
    console.error("Error in main routine: " + mainException);
    process.exit(1);
}

function pluckGroup(tagArray, messageType, groupName, numInGroup) {

    var groupAnchor;
    var group = [];
    var member = {};
    var firstProp = undefined;
    var idx = 0;
    var groupFields = GROUPS[messageType][groupName];

    if (tagArray && tagArray.length > 0) {
        groupAnchor = tagArray[0].tag;
    } else {
        console.error('empty tag array found in pluckGroup');
        return [];
    }

    while (tagArray.length > 0) {

        var tag = tagArray.shift();
        var key = tag.tag;
        var val = tag.val;
        var num = tag.num;

        var tagInGroup = _.contains(groupFields, key);
        var type;

        if (TAGS[num]) {
            type = TAGS[num].type ? TAGS[num].type : 'STRING';
        } else {
            type = 'STRING';
        }

        if (idx > 0 && key === groupAnchor) { // add current member to group, reset member
            group.push(_.clone(member));
            member = {};
            member[key] = val;
        } else if (type === 'NUMINGROUP' || key.substring(0, 2) === 'No') { // recurse into new repeating group
//          } else if (key.substring(0, 2) == 'No') { // recurse into new repeating group
            console.log('GROUP: ' + key);// JSON.stringify(newGroup));
            member[key] = val;
            var newGroup = pluckGroup(tagArray, messageType, key, val);
            member[key.substring('No'.length)] = newGroup;
        } else if (!tagInGroup) { // we've reached the end of the group
            group.push(_.clone(member)); // add the last processed member to the group
            tagArray.push(tag); // put this guy back, he doens't belong here
            return group;
        } else {
            member[key] = val; // tag is a member of an in-flight group
        }

        idx++;

    }

}

function resolveFields(fieldArray, dict) {

    targetObj = {};
    var group = [];

    while (fieldArray.length > 0) {
        
        var field = fieldArray.shift();
        var key = field.tag;
        var val = field.val;
        var raw = field.raw;
        var num = field.num;

        console.log(util.inspect(field, undefined, null));
        
        if (isGroup(field, dict.fields)) {
            targetObj[key] = val;
            
          //  console.log('GROUP: ' + util.inspect(field, undefined, null));
        } //else {
            targetObj[key] = val;
        //}

    }

//    console.log(util.inspect(targetObj, undefined, null));
    process.exit();
    return targetObj;

}

function processLine(line, options, dict) {
    var targetObj = resolveFields(extractFields(line, dict), dict);
    if (options.yaml) {
        return YAML.stringify(targetObj, 256);
    } else {
        return pretty ? JSON.stringify(targetObj, undefined, 2) : JSON.stringify(targetObj)
    }
}

function extractFields(record, dict) {
    let fieldArray = [];
    let fields = record.split(delim);
    for (var i = 0; i < fields.length; i++) {
        var both = fields[i].split('=');
        both[0].replace("\n", '').replace("\r", '');
        if (both[1] !== undefined && both[0] !== undefined) {
            let fieldNum = both[0].trim();
            let rawVal = both[1].trim();
            let fieldDef = _.findWhere(dict.fields, {
                number: fieldNum
            });
            let tag = fieldDef ? fieldDef.name : fieldNum;
            let val = (fieldDef ? (fieldDef.values ? mnemonify(fieldDef.values, rawVal) : rawVal) : rawVal); 
            let field = {
                tag: tag,
                val: val,
                num: fieldNum,
                raw: rawVal
            }
            fieldArray.push(field);
        }
    }
    return fieldArray;
}

function versionFive(fieldArray) {
    return _.findWhere(fieldArray, { tag: 'ApplVerID' }) != undefined; 
}

function mnemonify(values, raw) {
    let value = _.findWhere(values, { enum: raw });
    return value ? value.description.replace(/_/g, ' ') : raw;
}



function flattenComponent(componentName, dom) {
    var fieldNames = [];
    var components = xpath.select('//fix/components/component', dom);

    if (!components || components.length === 0) {
        console.error('could not find component: ' + componentName);
        return fieldNames;
    } else {
        for (var i = 0; i < components.length; i++) {
            var fields = components[i].getElementsByTagName('field');
            for (var j = 0; j < fields.length; j++) {
                fieldNames.push(fields[j].attributes[0].value);
            }

            var comps = components[i].getElementsByTagName('component');
            for (var k = 0; k < comps.length; k++) {
                var compName = comps[k].attributes[0].value;
                fieldNames.push(flattenComponent(compName, dom));
            }
        }
        return _.uniq(fieldNames);
    }
}

function isGroup(field, fields) {
    fieldDef = _.findWhere(fields, { number: field.num });
    return fieldDef ? (fieldDef.type === 'NUMINGROUP' ? true : false) : false;
}

function componentGroups(componentNode) {

    let groups = [];
    
    return ;
}

function dictionaryGroups(dom) {

    let componentPath = FIX_VER.split('.')[0] === '5' ? '//fix/components/component' : '//fix/messages/message/group';

    //    var components = xpath.select('//fix/components/component', dom);
    var components = xpath.select(componentPath, dom);

    var componentGroupFields = {};

    for (var j = 0; j < components.length; j++) {

        var componentName = components[j].attributes[0].value;
        componentGroupFields[componentName] = {};
        var componentGroups = components[j].getElementsByTagName('group');

        for (var k = 0; k < componentGroups.length; k++) {
            var componentGroupName = componentGroups[k].attributes[0].value;
            componentGroupFields[componentName][componentGroupName] = [];
            var groupFields = componentGroups[k].getElementsByTagName('field');

            for (var l = 0; l < groupFields.length; l++) {
                var fieldName = groupFields[l].attributes[0].value;
                componentGroupFields[componentName][componentGroupName].push(fieldName);
            }

            var groupComponents = componentGroups[k].getElementsByTagName('component');
            for (l = 0; l < groupComponents.length; l++) {
                var compName = groupComponents[l].attributes[0].value;
                componentGroupFields[componentName][componentGroupName] = componentGroupFields[componentName][componentGroupName].concat(flattenComponent(compName, dom));
            }

        }

    }

    var names = messageNames(dom);
    var messages = xpath.select('//fix/messages/message', dom);

    for (var m = 0; m < messages.length; m++) {
        var messageName = messages[m].attributes[0].value;
        GROUPS[messageName] = {};

	      // need to fork logic here based on whether we are working with a 4.x or 5.x 
	      // message format

        var messageComponents = messages[m].getElementsByTagName('component');

        for (var n = 0; n < messageComponents.length; n++) {
            var componentName = messageComponents[n].attributes[0].value;
            var groupNames = Object.keys(componentGroupFields[componentName]);

            for (o = 0; o < groupNames.length; o++) { // collapse fields into GROUPS index
                GROUPS[messageName][groupNames[o]] = componentGroupFields[componentName][groupNames[o]];
            }
        }
    }
}

function getFixVer(dom) {

    var fixMaj = xpath.select("//fix/@major", dom)[0].value;
    var fixMin = xpath.select("//fix/@minor", dom)[0].value;
    var fixSp = xpath.select("//fix/@servicepack", dom)[0].value;
    FIX_VER = [fixMaj, fixMin, fixSp].join('.');

}

function messageNames(dom) {

    var messages = [];
    var path = '//fix/messages/message';
    var msgs = xpath.select(path, dom);

    for (var i = 0; i < msgs.length; i++) {
        messages.push({
            type: msgs[i].attributes[2].value,
            name: msgs[i].attributes[0].value
        });
    }

    MESSAGES = messages;
    

}

function readDataDictionary(fileLocation) {

    var xml = fs.readFileSync(fileLocation).toString();
    var parseString = require('xml2js').parseString;
    var dict = {};
    
    parseString(xml, function (err, datadict) {
        if (!err) {
            dict = makeDict(datadict.fix.messages[0].message,
                            datadict.fix.components[0].component,
                            datadict.fix.fields[0].field);
        } else {
            console.error(JSON.stringify(err));
            process.exit(1);
        }
    });

    return dict;
}

function makeDict(messages, components, fields) {

    let dict = {};
    let msgs = [];
    let comps = [];
    let flds = [];

    _.each(messages, function(value, key, list) {
        let msg = value.$
        msg.fields = value.field;
        msg.components = value.component;
        msgs.push(msg);
    });

   _.each(components, function(value, key, list) {
       let comp = value.$;
       comp.fields = value.field;
       comps.push(comp);
    });

    _.each(fields, function(value, key, list) {
        let fld = value.$;
        let values = [];
        _.each(value.value, function(v, k, l) {
            values.push(v.$);
        });
        fld.values = values;
        flds.push(fld);
    });
    
    dict.messages = msgs;
    dict.components = comps;
    dict.fields = flds;

    return dict;
    
}
function flatten(component, dict) {
}

function checkParams() {

    let options = {};

    if (process.argv.length < 3) {
        console.error("Usage: fix2json [-p] <data dictionary xml file> [path to FIX message file]");
        console.error("\nfix2json will use standard input in the absence of a message file.");
        process.exit(1);
    } else if (process.argv.length === 3) {
        dictname = process.argv[2];
    } else if (process.argv.length === 4) {
        if (process.argv[2] === '-p') {
            options.pretty = true;
            options.dict = process.argv[3];
        } else {
            options.dict = process.argv[2];
            options.file = process.argv[3];
        }
    } else if (process.argv.length === 5) {
        pretty = true;
        options.dict = process.argv[3];
        options.file = process.argv[4];
    }
    if (process.argv[1].indexOf('yaml') > 0) {
        options.yaml = true;
    }

    return options;

}
