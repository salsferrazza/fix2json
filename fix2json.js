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
                console.log(YAML.stringify(msg));
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

//    console.log(JSON.stringify(dict));
  //  process.exit(0);
    
    return dict;

}

function makeObject(fieldList, dict, callback) {

}

function resolveField(field, dict, callback) {
    let fld = { tag: field.tag , 
                val: undefined };
    const def = _.findWhere(dict.fields, { name: field.tag });
    if (def) {
        if (def.type === 'NUMINGROUP') {
            targetObj[field.tag] = field.raw;
            makeGroup(fieldList, def, field, dict, function (group, fieldsLeft) {
                callback(group, fieldsLeft);
            });
        } else {
            callback(mnemonify(def, field.raw));
        }
    } else {
        callback(field.raw);
    }
    return;
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
        const fieldDef = _.findWhere(dict.fields, { name: field.tag });
        console.log('got fieldDef ' + util.inspect(fieldDef) + '/' + util.inspect(field));
        console.log(field.tag + ' def: ' + YAML.stringify(_.findWhere(groupDef.fields, { name: field.tag })));
        
        if (!anchor) {

            anchor = field.tag;
            console.log('set anchor to ' + anchor);
            item[field.tag] = field.val;
            console.log('item: ' + util.inspect(item));
            
                    
        } else if (field.tag === anchor) {

            console.log('new item: ' + util.inspect(item));
            items.push(JSON.parse(JSON.stringify(item))); // TODO: fix ugly
            item = {};
            item[field.tag] = field.val;
            console.log('item: ' + util.inspect(item));
            
        } else if (fieldDef.type === 'NUMINGROUP') { // nested group

            item[field.tag] = field.raw;
            console.log('nested group ' + field.tag);
            makeGroup(fieldList, msgDef, field.tag, dict, function (group, remainingFields) {
                items[field.tag.substring('No'.length)] = group;
                fieldList = remainingFields;
            });
        
        } else if (!_.findWhere(groupDef.fields, { name: field.tag })) {

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

    console.log('grp items: ' + JSON.stringify(items));
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

function componentFields(componentName, dict) {

    let fields = [];

    const compDef = _.findWhere(dict.components, { name: componentName });
//    console.log(componentName + ': ' + JSON.stringify(compDef));

    if (!compDef) { return fields; } 
    
    if (compDef && compDef.group) {
        const grp = prune(compDef.group);
        fields = fields.concat(prune(grp.field));
//        console.log('compf: ' + YAML.stringify(fields));

        if (compDef.group.component) {
            compDef.group.component.forEach(function (cmp) {
                fields = fields.concat(componentFields(cmp.name, dict));
            });
        }
    }
    
    if (compDef && compDef.field) {
        fields = fields.concat(prune(compDef.field));
//        console.log('compf: ' + YAML.stringify(fields));
    }
        
    if (compDef && compDef.component) {
        _.each(prune(compDef.component), function (val, key, list) {
            fields = fields.concat(componentFields(val.name, dict)); 
           // console.log('compf: ' + YAML.stringify(fields));
        });
    }

//    console.log('returning from cf: ' + YAML.stringify(fields));
    return fields;
}

function groupFields(msgDef, groupName, dict) {
    // pass in group
    // get components and fields
    // copy fields 
    let fields = [];
    // const cmpList = prune(msgDef.component);
//    console.log('fields for: ' + groupName);
    const groupDef = findGroupDef(groupName, msgDef, dict);
  //  console.log('gf: ' + YAML.stringify(groupDef, 1, 1));

    if (groupDef && groupDef.field) {
        fields = fields.concat(prune(groupDef.field));
    }

    if (groupDef && groupDef.component) {
        _.each(prune(groupDef.component), function (v, k, l) {
            if (v.field) {
                fields = fields.concat(prune(v.field));
            }
            if (v.component) {
                fields = fields.concat(componentFields(prune(v.component)));
            }
        });
    }

//    console.log('group fields: ' + YAML.stringify(fields));
    
    return fields;
}


function findGroupDef(groupName, msgDef, dict) {
    
    const def = _.findWhere(dict.messages, { name: msgDef.name });
    console.log('fgf msg: ' + YAML.stringify(def));
    const comps = prune(msgDef.component);

    if (comps && comps.length > 0) {
        for (let i = 0; i < comps.length; i++) {
            const comp = comps[i];
            console.log('comp: ' + JSON.stringify(comp));
            const compDef = _.findWhere(dict.components, { name: comp.name });
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
                console.log('no compdef found for ' + comp.name);
                continue;
            }
        }
    } else {
        console.log('msgDef ' + msgDef.name + ' has no components');
    }
}

function structure(fieldArray, dict) {

    const MSGTYPE_TAG = 35;
    let targetObj = {};
    let fieldList = fieldArray.slice(0);

//    console.log(JSON.stringify(fieldList, 2, 2));
    
    const type = _.findWhere(fieldList, { num: MSGTYPE_TAG })
    const msgDef = _.findWhere(dict.messages, { msgtype: type.raw });
/*
    if (msgDef) {

        _.each(prune(msgDef.field), function (v, k, l) {


            const fieldDef = _.findWhere(dict.fields, { name: v.name })
                  || { number: field.num,
                       name: field.tag,
                       type: STRING };

            console.log(v.name + ': ' + JSON.stringify(fieldDef));

            const val = _.findWhere(fieldList, { tag: v.name });

            if (val) {
                targetObj[v.name] = mnemonify(fieldDef, val.val);
            }

        });
        
    }

  */  
    
    if (msgDef) {
        
        while (fieldList.length > 0) {

            const field = fieldList.shift(); // get next field from array

            // get definition of the field in dictionary
            // if not found, return a stub fieldDef
            const fieldDef = _.findWhere(dict.fields, { name: field.tag })
                  || { number: field.num,
                       name: field.tag,
                       type: STRING };

        //    console.log(JSON.stringify(field) + ': ' + JSON.stringify(fieldDef, 2, 2));
            
            // if this field represents a group, then we
            // need to branch out and deal with it
            if (fieldDef.type === 'NUMINGROUP') {

//                console.log('group coming up: ' + YAML.stringify(field));
//                console.log(field.tag + ' def: ' + JSON.stringify(findGroupDef(field.tag, msgDef, dict)));
                
                // add original No* group field to document
                targetObj[field.tag] = field.val;

                // manufacture array to represent group (recursively)
                const grp = makeGroup(fieldList,
                                      msgDef,
                                      field.tag,
                                      Number(field.val),
                                      dict);

                console.log('back from make group: ' + YAML.stringify(grp));
                
                // trim the No* prefix off for the group property name
                targetObj[field.tag.substring('No'.length)] = grp ? grp.group : {};

                // we've exhausted fields out of the original array
                // so we'll reset the main fieldArray to reflect this
//                fieldList = grp.fieldsLeft;

               // console.log('post group: ' + JSON.stringify(grp.fieldsLeft, 2, 2));

            } else {
                console.log('set ' + field.tag + ' to ' + mnemonify(fieldDef, field.val));
                targetObj[field.tag] = mnemonify(fieldDef, field.val);
            }           
        }
    } else {
        console.error('No definition found for message type ' + type.raw);
    }
    return targetObj;
}

function mnemonify(fieldDef, raw) {        
    if (!fieldDef || !fieldDef.value) {
        return raw;
    } else {
//        if (fieldDef.type === 'NUMBER') {
  //          return Number(
    //    }
        let value = _.findWhere(prune(fieldDef.value), { enum: raw });
        return value ? value.description.replace(/_/g, ' ') : raw;
    }
}
