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
            //console.log(msg);
        }
    });

} catch (mainException) {
    console.error("Error in main routine: " + mainException.stack);
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

function prune(array) {

    let output = [];

    _.each(array, function (value, key, list) {
         output.push(value.$);
    });

    return output;

}

function readDataDictionary(fileLocation) {

    var xml = fs.readFileSync(fileLocation).toString();
    var parseString = require('xml2js').parseString;
    var dict = {};
    
    parseString(xml, function (err, datadict) {
//        console.log(util.inspect(datadict, undefined, null));
        if (!err) {
            dict = makeDict(datadict);
        } else {
            console.error(JSON.stringify(err));
            process.exit(1);
        }
    });

    return dict;
}

function makeDict(datadict) {

    //console.log(util.inspect(datadict, undefined, null));
    
    let messages = datadict.fix.messages[0].message;
    let fields = datadict.fix.fields[0].field;
    let version = Number(datadict.fix.$.major);
    let components, groups = undefined;
    console.log(version);
    if (version === 5) {
        components = datadict.fix.components[0].component;
    } else {
        groups = datadict.fix.fields[0].group;
    }
    
    let dict = {};
    let msgs = [];
    let flds = [];

    // process message defs
    _.each(messages, function(value, key, list) {


        let items = [];
        let msg = value.$
        msg.fields = prune(value.field);

        let bag = version == 4 ? prune(value.group) : prune(value.component);
        msg[version === 4 ? 'groups' : 'components'] = bag;
        /*   _.each(bag, function(value, key, list) {
            let item = ;
            item.fields = value.field;
            items.push(item);
        });

        msg[(version === 4 ? 'groups' : 'components')] = prune(items);
     */
        console.log('msg: ' + util.inspect(msg, undefined, null));
        msgs.push(msg);

    });

    // process field defs
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
    dict.fields = flds;

    console.log(JSON.stringify(dict, undefined, 2));
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

function makeGroup(fieldArray, messageDefs, groupNum) {

    let index = 0;
    let grpVals = [];
    let anchor = undefined;
    let grp = {};
    
    while (fieldArray.length > 0) {

        let field = fieldArray.shift();
       
        if (index === 0) {
            anchor = field.tag;
            grp[field.tag] = field.val;
        } else if (field.tag === anchor) {
            grpVals.push(grp);
            grp = {};
        } else if (isGroup(field, messageDefs)) {
            console.log('fields before mkg: ' + util.inspect(fieldArray, undefined, null));
            grp = makeGroup(fieldArray, messageDefs, field.num);
            grp[field.tag.substring(2)] = grp;
        } else {
                grp[field.tag] = field.val;
        }

        index++;
        
    }
    
}

function validFields(dict, groupTagNumber) {

    // find message children
    // for each component, drill down and flatten recursively
    // for each group, pull out child fields and flatten

    let fields = [];

    console.log('find ' + groupTagNumber + ' in ' + util.inspect(Object.keys(dict.fields), undefined, null));

    _.each(dict.fields, function (value, key, list) {
        
        
        
    });
    
    return fields;
    
    
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
            let grp = makeGroup(fieldArray, dict.messages, field.num);
            targetObj[key.substring(2)] = grp; // Strip prefix 'No' from numingroup
            //console.log('GROUP: ' + field.tag + '/' + field.val);
        } else {
            
        }

    }

    return targetObj;

}


function pluckGroup(tagArray, messageType, groupName) {

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
            var newGroup = pluckGroup(tagArray, messageType, key);
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
    let fieldDef = _.findWhere(fields, { number: field.num });
    fieldDef = _.findWhere(fields, { number: field.num });
    return fieldDef ? (fieldDef.type === 'NUMINGROUP' ? true : false) : false;
}


function flatten(component, dict) {

}

