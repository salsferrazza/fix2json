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

    let rd = readline.createInterface({
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

function readDataDictionary(fileLocation) {

    var xml = fs.readFileSync(fileLocation).toString();
    var parseString = require('xml2js').parseString;
    var dict = {};
    
    parseString(xml, function (err, datadict) {
        if (!err) {
            
     
            dict = makeDict(datadict.fix.messages[0].message,
                            datadict.fix.components[0].component || undefined,
                            datadict.fix.fields[0].field);
            dict.version = Number(datadict.fix.$.major); // 4.x has groups & fields, 5.x has components & field
            console.log(util.inspect(dict.messages, undefined, null));
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

function processLine(line, options, dict) {
    var targetObj = resolveFields(extractFields(line, dict), dict);
    if (options.yaml) {
        return YAML.stringify(targetObj, 256);
    } else {
        return options.pretty ? JSON.stringify(targetObj, undefined, 2) : JSON.stringify(targetObj)
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


function resolveFields(fieldArray, dict) {

    targetObj = {};
    var group = [];

    while (fieldArray.length > 0) {
        
        var field = fieldArray.shift();
        var key = field.tag;
        var val = field.val;
        var raw = field.raw;
        var num = field.num;

        
        
        targetObj[key] = val;

        if (isGroup(field, dict.fields)) {

            console.log('GROUP: ' + field.tag + '/' + field.val);


        }

    }

    return targetObj;

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


function groupFields(tagNumber, dict) {

//    let msgDef = _.findWhere(dict.fie
    
}


function mnemonify(values, raw) {
    let value = _.findWhere(values, { enum: raw });
    return value ? value.description.replace(/_/g, ' ') : raw;
}

function isGroup(field, fields) {
    fieldDef = _.findWhere(fields, { number: field.num });
    return fieldDef ? (fieldDef.type === 'NUMINGROUP' ? true : false) : false;
}


function flatten(component, dict) {

}

