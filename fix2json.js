#! /usr/bin/env node --harmony

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
const pretty = false;
const yaml = false;

// some of these TODO's below are very speculative:
//
// TODO: decouple logic from file ingestion, but if this was a browser module, how would we package the dictionaries?
// TODO: get dictionary management out of this module
// TODO: autodetect FIX version from source data?
// TODO: emit pre and post processing events for each message processed
// TODO: forward engineer JSON to FIX?  Would be pretty useful for browser UI's based on FIX dictionaries

if (process.stdout._handle) process.stdout._handle.setBlocking(true)

const delim = String.fromCharCode(01); // ASCII start-of-header
const options = checkParams();

try {
    
    let dict = readDataDictionary(options.dictpath);

  //  console.log(JSON.stringify(flatten('UndInstrmtGrp', dict)));
//    process.exit(0);
    
    let input;
    if (options.file) {
        if (options.file.substring(options.file.length - 3).toLowerCase() === '.gz') {
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
            processLine(line, options, dict, function (msg) {
//                decoder.write(
                console.log(YAML.stringify(msg, 5, 1));
            });
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
        options.dictpath = process.argv[2];
    } else if (process.argv.length === 4) {
        if (process.argv[2] === '-p') {
            options.pretty = true;
            options.dictpath = process.argv[3];
        } else {
            options.dictpath = process.argv[2];
            options.file = process.argv[3];
        }
    } else if (process.argv.length === 5) {
        options.pretty = true;
        options.dictpath = process.argv[3];
        options.file = process.argv[4];
    }
    if (process.argv[1].indexOf('yaml') > 0) {
        options.yaml = true;
    }

    return options;

}

function prune(array) {
    
    if (!array) { return undefined }
    let output = [];
    _.each(array, function (value, key, list) {
        let newObj = _.clone(value.$);
        _.each(Object.keys(value), function (v, k, l) {
            if (v !== '$') {
                newObj[v] = value[v];
            }
        });
        output.push(newObj);
    });
    return output; 
}

function readDataDictionary(fileLocation) {

    var xml = fs.readFileSync(fileLocation).toString();
    var parseString = require('xml2js').parseString;
    var dict = {};
    
    parseString(xml, function (err, datadict) {
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

    let messages = prune(datadict.fix.messages[0].message);
    let fields = prune(datadict.fix.fields[0].field);
    let components = prune(datadict.fix.components[0].component);
    let version = Number(datadict.fix.$.major);

    let dict = {};
    let fieldMap = [];

    dict.messages = messages;
    dict.fields = fields;
    dict.components = components;
    
    return dict;

}

function hasField(groupDef, fieldName) {

    if (_.findWhere(groupDef.fields, { name: fieldName })) {
        console.log('found ' + fieldName + ' in ' + util.inspect(groupDef.fields));
        return true;
    }    
    if (groupDef.components) {
        for (let i = 0; i < groupDef.components; i++) {
            const cmp = groupDef.components[i];
            if (_.findWhere(cmp.field, { name: fieldName })) {
                console.log('found ' + fieldName + ' in ' + util.inspect(groupDef.components));
                return true;
            }
        }
    }
    return false;    
}

function flatten(componentName, dict) {

    let fields = [];
    const comp = _.findWhere(dict.components, { name: componentName });

    if (!comp) { return undefined; };

    if (comp.field) {
        for (let i = 0; i < comp.field.length; i++) {
            fields.push(comp.field[1].name);
        }
    }
    
    if (comp.component) {
        
        for (let j = 0; j < comp.component.length; i++) {
            if (comp.component[j]) {
                fields.push(comp.component[j].field);
            }
            for (let k = 0; k < comp.component[j].component.length; k++) {
                fields.push(flatten(comp.component[j].component.name));
            }
        }
    }
    return fields;
}

function makeGroup(fieldArray, msgDef, groupName, dict, callback) {

    const groupDef = findGroupDef(groupName, msgDef, dict);
                             
    
    if (!groupDef) {
        console.log('NO GROUP DEF FOR ' + groupName);
        callback(undefined);
        return;
    } else {
        console.log('groupdef: ' + YAML.stringify(groupDef));
    }
    
    let fieldList = fieldArray.slice(0);
    let anchor = undefined;
    let items = [];
    let item = {};

    while (fieldList.length > 0) {
                         
        const field = fieldList.shift();

        console.log(groupName + ' has field ' + field.tag + ': ' + hasField(groupDef, field.tag)); 
 

        const fieldDef = _.findWhere(dict.fields, { name: field.tag });
        if (!anchor) {
            anchor = field.tag;
            console.log('set anchor to ' + anchor);
            item[field.tag] = field.val;
            console.log('item: ' + util.inspect(item));
        } else if (field.tag === anchor) {
            items.push(JSON.parse(JSON.stringify(item))); // TODO: fix ugly
            item = {};
            item[field.tag] = field.val;
            console.log('new item: ' + util.inspect(item));
        } else if (fieldDef.type === 'NUMINGROUP') { // nested group
            item[field.tag] = field.raw;
            console.log('nested group ' + field.tag);
            makeGroup(fieldList, msgDef, field.tag, dict, function (group, remainingFields) {
                items[field.tag.substring('No'.length)] = group;
                fieldList = remainingFields;
            });
//        } else if (!_.findWhere(groupDef.fields, { name: field.tag })) {
        } else if (!hasField(groupDef, field.tag)) {
            console.log('\n\n' + field.tag + ' not in ' + JSON.stringify(groupDef) + '\n\n');
            fieldList.push(field);
            console.log('item: ' + util.inspect(item));
            items.push(JSON.parse(JSON.stringify(item)));
            console.log('fields left: ' + YAML.stringify(fieldList));
            callback(items, fieldList);
            return;
        } else {
            console.log('normal: ' + field.tag + ' = ' + field.val);
            item[field.tag] = field.val;
        }
    }
    callback(items, fieldList);
    return;
}

function processLine(line, options, dict, callback) {
    extractFields(line, dict, function (fieldArray) {
        let targetObj = {};
        const type = _.findWhere(fieldArray, { num: 35 });
        const msgDef = _.findWhere(dict.messages, { msgtype: type.raw });
        if (!type) {
            console.error('type not found: ' + YAML.stringify(fieldArray));
            callback(undefined);
            return;
        } else if (!msgDef) {
            console.error('msgDef not found: ' + YAML.stringify(fieldArray));
            callback(undefined);
            return;
        }
        let fieldList = fieldArray.slice(0);
        while (fieldList.length > 0) {
            const field = fieldList.shift();
            const def = _.findWhere(dict.fields, { name: field.tag });
            if (def && def.type === 'NUMINGROUP') {
                targetObj[field.tag] = Number(field.raw);
                makeGroup(fieldList, msgDef, field.tag, dict, function (group, fieldsLeft) {
                    console.log('bck from mg: ' + YAML.stringify(group));                        
                    targetObj[field.tag.substring('No'.length)] = group;
                    fieldList = fieldsLeft ? fieldsLeft.slice(0) : [];
                    console.log('fieldleft: ' + JSON.stringify(fieldList));
                });
            } else {
                targetObj[field.tag] = field.val;
            }
        }
        callback(targetObj);
        return;
    });
}

function extractFields(record, dict, cb) {
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
            let val = mnemonify(fieldDef, rawVal); 
            let field = {
                tag: tag,
                val: val,
                num: Number(fieldNum),
                raw: rawVal
            }
            fieldArray.push(field);
        }
    }
    cb(fieldArray);
    return;
}


function findGroupDef(groupName, msgDef, dict) {
    
    const def = _.findWhere(dict.messages, { name: msgDef.name });
    console.log('gdf msg: ' + YAML.stringify(def));
    const comps = prune(msgDef.component);
    const grps = prune(msgDef.group);

    if (comps && comps.length > 0) {
        for (let i = 0; i < comps.length; i++) {
            const comp = comps[i];
            console.log('comp: ' + JSON.stringify(comp, 1, 1)); 
            const compDef = _.findWhere(dict.components, { name: comp.name })
            console.log('comp def: ' + JSON.stringify(compDef, 1, 1));
            
            if (compDef && compDef.group) {
                const grps = prune(compDef.group);
                for (let j = 0; j < grps.length; j++) {                    
                    const grp = grps[j];
                    if (grp.name === groupName) {                    
                        return { 
                            fields: prune(grp.field), 
                            components: prune(grp.component)
                        };
                    } else {
                        console.log(grp.name + ' != ' + groupName);
                        continue;
                    }
                }
            } else {
                console.log('no groups found in component ' + comp.name);
                continue;
            }
        }
    }

    if (grps && grps.length > 0) {
        console.log('size of ' + msgDef.name + ' groups is ' + grps.length);
        for (let i = 0; i < grps.length; i++) {
            const grp = grps[i];
            console.log('found group: ' + grp.name + '(' + groupName + ')');
            if (grp.name === groupName) {
                return { 
                    fields: prune(grp.field), 
                    components: prune(grp.component)
                };
            } else {
                console.log(grp.name + ' != ' + groupName);
            }
        }
    }
}

function mnemonify(fieldDef, raw) {        
    if (!fieldDef || !fieldDef.value) {
        return raw;
    } else {
        let value = _.findWhere(prune(fieldDef.value), { enum: raw });
        return value ? value.description.replace(/_/g, ' ') : raw;
    }
}
