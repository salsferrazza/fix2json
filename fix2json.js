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
    //  console.log(JSON.stringify(dict, undefined, 2));
    //    process.exit();
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

    //    console.log(JSON.stringify(dict, undefined, 2));
    return dict;
}

function makeDict(datadict) {
    
    let messages = prune(datadict.fix.messages[0].message);
    let fields = prune(datadict.fix.fields[0].field);
    let components = prune(datadict.fix.components[0].component);
    let version = Number(datadict.fix.$.major);

    let dict = {};
    dict.messages = messages;
    dict.fields = fields;
    dict.components = components;

  //  _.each(components, function (v, k, l) {
//        console.log('component fields for ' + v.name + ': ' + util.inspect(componentFields(v.name, dict)));
    //});

//    process.exit(0);
    return dict;
    
}

function processLine(line, options, dict) {
    var targetObj = structure(extractFields(line, dict), dict);
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
                num: Number(fieldNum),
                raw: rawVal
            }
            fieldArray.push(field);
        }
    }
    console.log(JSON.stringify(fieldArray, 2, 2));
    return fieldArray;
}

function makeGroup(fieldArray, dict, groupDef) {
    let index = 0;
    let grpVals = [];
    let anchor = undefined;
    let grpItem = {};

 //   let grpDef = _.findWhere(dict.groups, { name: groupNum })
    let grpFlds = prune(groupDef.field);
    
    // assumes the field array passed starts with the
    // first tag of the first item in the group
    // and contains the entire rest of the message
    
    while (fieldArray.length > 0) {

        let field = fieldArray.shift();

        if (index === 0) {
            console.log('group anchor field is ' + field.tag);
            anchor = field.tag;
            grpItem[field.tag] = field.val;
        } else if (field.tag === anchor) {
            console.log('adding new item to group');
            grpVals.push(grpItem);
            grpItem = {};
            grpItem[field.tag] = field.val;
        } else if (isGroup(field, dict.fields)) {
            console.log('found subgroup: ' + field.tag);
            let obj =  makeGroup(fieldArray, dict, _.findWhere(prune(groupDef.group), { name: field.tag }));
            grpItem[field.tag.substring('No'.length)] = obj.group;
            fieldArray = obj.fieldsLeft;
        } else if (!_.findWhere(grpFlds, { name: field.tag }))  {
            console.log(field.tag + ' not found in group ' + util.inspect(grpFlds, undefined, null) + ', this group is done');
            grpVals.push(grpItem);
            fieldArray.push(field);
            return { group: grpVals,
                     fieldsLeft: fieldArray};
        } else {
            console.log('just adding property to item');
            grpItem[field.tag] = field.val;
        }

        index++;

    }

    console.log('group made: ' + util.inspect(grpVals, undefined, null));
    return { group: grpVals,
             fieldsLeft: fieldArray };
    
}


// returns 
function flatten(groupOrComponent, msgDef, dict) {
    
    let outFields = [];

    if (groupOrComponent.field) { // sub-fields for this message def, may be 0
        outFields = prune(groupOrComponent.field);
    }

    let groups = prune(groupOrComponent.group); // groups may be wrapped in a component, 1-to-1
    if (groups) {                               
        if (groups != undefined) {
            console.log("flatten: found group " + util.inspect(groups, undefined, null));
            _.each(groups, function (v, k, l) {
                outFields = outFields.concat(flatten(v, msgDef, dict)); 
            });
        }
    }

    let comps = prune(groupOrComponent.component); // components have groups/fields, and/or other components
    if (comps) {
        if (comps != undefined) {
            console.log("flatten: found component " + util.inspect(comps, undefined, null));
            _.each(comps, function (v, k, l) {
                outFields = outFields.concat(flatten(v, msgDef, dict));
            });
        }
    }
    
    return outFields;
    
}

function componentDefiniton(name, dict) {

    return prune(_.findWhere(dict.components, { name: name }));
    
}

function pluckGroup(groupName, fields, dict) {

    let group = {};
    let fieldsLeft = [];
    let inGroup = true;
    let idx = 0;
    
    while (inGroup) {

        let field = fields.shift;
        let anchor = undefined;
        let groupItem = {};
        
        if (!anchor) {
            anchor = field.tag;
            
            groupItem[field.tag] = field.val;
        } else {
            
        }
        
        idx++;
        
    }

    
    return { group: groupObj,
             fields: fieldsLeft }
    
}

function structure(fieldArray, dict) {

    let MSGTYPE_TAG = 35;
    let targetObj = {};
    let group = [];

    let type = _.findWhere(fieldArray, { num: 35 })
    let msgDef = _.findWhere(dict.messages, { msgtype: type.raw });
    if (!msgDef) { 
        console.error('Could not find message type ' + type.raw + ' in data dictionary ' + options.file);
        return fieldArray;
    } else {
        console.log(util.inspect(msgDef, undefined, null));
    }
    let grps = prune(msgDef.group);
    let cmps = msgDef.component ? prune(msgDef.component) : [];
    
    console.log('grps for ' + msgDef.name + ' are ' + util.inspect(grps, undefined, null));
    console.log('cmps for ' + msgDef.name + ' are ' + util.inspect(cmps, undefined, null));

    _.each(cmps, function (v, k, l) {
        console.log('component grps for ' + v.name + ': ' + util.inspect(componentGroups(v.name, dict), undefined, null));
        console.log(v.name + ': ' + JSON.stringify(componentFields(v.name, dict)));
    });

    
    while (fieldArray.length > 0) {

        let field = fieldArray.shift();
        let fieldDef = _.findWhere(dict.fields, { number: String(field.num) });
        console.log('fielddef: ' + util.inspect(fieldDef, undefined, null));
        console.log('fieldval: ' + util.inspect(field, undefined, null));
        
//        if (fieldDef && fieldDef.type === 'NUMINGROUP') {
//            console.log('this a group: ' + field.tag + ' | ' + JSON.stringify((grps ? grps : componentGroups(cmps)), undefined, null))

        let groupDef;
        if (groupDef = isGroup(grps, field.tag)) {
                    
            if (!groupDef && cmps.length > 0) {
                // check to see if the group is defined underneath one of the component definitons for the message

                let g = componentGroups(field.tag, dict);
                console.log('cmp grp: ' + util.inspect(g, undefined, null));

                if (g.length > 0) {
                    _.each(g, function (v, k, l) {
                        console.log('grp: ' + util.inspect(v, undefined, null));
                    });
                }
                                
            }
            
            if (groupDef) {
                                           
                console.log('grpdef: ' + util.inspect(groupDef, undefined, null));
                console.log('grpfld: ' + util.inspect(groupFields(groupDef.name, dict), undefined, null));

                let fields = prune(groupDef.field);
                let components = prune(groupDef.component);

                
                targetObj[fieldDef.name] = field.val;
                let obj = makeGroup(fieldArray, dict, groupDef);
                console.log('adding group: ' + fieldDef.name.substring(2) + ' as ' + util.inspect(obj.group, undefined, null));
                targetObj[fieldDef.name.substring(2)] = obj.group;
                console.log('fields left: ' + obj.fieldsLeft.length);
                fieldArray = obj.fieldsLeft;

            } else {
                console.log('WTF is the group?');
                let mnemonic = _.findWhere(prune(fieldDef.value), { enum: field.raw });
                targetObj[fieldDef.name] = mnemonic ? mnemonic.value : field.description;
            }
        } else {
            if (fieldDef && fieldDef.value) {
                let mnemonic = _.findWhere(prune(fieldDef.value), { enum: field.raw });
                targetObj[field.tag] = mnemonic ? mnemonic.val : field.val;
            } else {
                targetObj[field.tag] = field.raw;
            }
        }
    }
    return targetObj;
}

function groupFields(name, dict) {

    let flds = [];
    let grp = _.findWhere(dict.groups, { name: name });
    
    _.each(prune(grp.field), function (v, k, l) {
        flds.push(v);
    });
    _.each(prune(grp.group), function (v, k, l) {
        flds.concat(groupFields(v.name, dict));
    });
    _.each(prune(grp.component), function (v, k, l) {
        flds.concat(componentFields(v.name, dict));
    });

    return flds;
    
}

function componentGroups(name, dict) {

    
//    console.log('find ' + name + ' in ' + JSON.stringify(dict.components, 2, 2));
    
    let comp = _.findWhere(dict.components, { name: name });
    if (comp && comp.group) {
        return prune(comp.group);
    } else {
        return [];
    }
}

function componentFields(name, dict) {
    let comp = _.findWhere(dict.components, { name: name });
    let fields = [];
    if (comp && (comp.field || comp.component || comp.group)) {
        _.each(prune(comp.component), function (v, k , l) {
            fields.concat(componentFields(v, dict));
        });
        _.each(prune(comp.group), function (v, k , l) {
            fields.concat(groupFields(v, dict));
        });
        return comp.field ? prune(comp.field).concat(fields) : fields;
    } else {
        return [];
    }
}

function subComponents(name, dict) {
    let comps = _.findWhere(dict.components, { name: name });
    return prune(comps.component);
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
            //            console.log('GROUP: ' + key);// JSON.stringify(newGroup));
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


function isGroup(groupList, tag) {
    return _.findWhere(groupList, { name: tag });
}

