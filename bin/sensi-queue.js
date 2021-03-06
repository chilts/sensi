#!/usr/bin/env node
// ----------------------------------------------------------------------------
//
// Sensi - set of 'Infrastructure as a Service' utilities which run on NodeJS.
// Copyright (c) 2010-2011 AppsAttic Ltd
// Written by Andrew Chilton (chilts@appsattic.com)
//
// This file is part of Sensi.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by the Free
// Software Foundation, either version 3 of the License, or (at your option)
// any later version.
//
// This program is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
// FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for
// more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.
//
// ----------------------------------------------------------------------------

// requires
var sys = require('sys')
  , http = require('http')
  , url = require('url')
  , fs = require('fs');

// global vars :)
var queue = {};
var cfg = read_config_file( process.argv[2] );

// side effects to set things in 'queue'
setup_queues( cfg );

// if we have a cluster, set up pinging it's siblings
if ( cfg.cluster ) {
    cfg.nodes.forEach(function(v, i) {
        console.log('I have a sibling at: ' + v);

        // setup our delay times for this node
        var delay = [2, 3, 5, 7, 11, 13, 17];
        delay.forEach(function(v, i) {
            var other_i = Math.floor(Math.random() * delay.length);
            // swap these around
            var t = delay[other_i];
            delay[other_i] = delay[i];
            delay[i] = t;
        });

        var i = 0;
        var ping = function(v) {
            var server = v.split(':');
            var options = {
                'host' : server[0],
                'port' : server[1],
                'path' : '/ping',
                'method' : 'GET'
            };
            var req = http.request( options, function(res) {
                debug( 'alive?', 'ok (' + v + ")" );
            });
            req.on('error', function(e) {
                error( 'alive?', 'ping failed for ' + v + ': ' + e.message );
            });
            req.end();

            // call us again in a few seconds
            setTimeout(function() { ping(v); }, delay[i] * 1000);
            console.log(i, delay[i]);
            i = ( i + 1 ) % delay.length;
        };

        // start off the pinging
        ping(v);
    });
}

// create the webserver
http.createServer(function (req, res) {
    // get the different parts of the URL
    var parts = url.parse( req.url, true );
    if ( parts.query === null ) {
        parts.query = {};
    }

    switch ( parts.pathname ) {
    case '/add':
        op_add(req, parts, res);
        break;

    case '/get':
        op_get(req, parts, res);
        break;

    case '/ack':
        op_ack(req, parts, res);
        break;

    case '/_del':
        op_del(req, parts, res);
        break;

    case '/info':
        op_info(req, parts, res);
        break;

    case '/ping':
        op_ping(req, parts, res);
        break;

    default:
        write_result(res, 404, 404, 'Not Found');
        break;
    }

}).listen(cfg.port);

sys.puts('Server listening on http://127.0.0.1:' + cfg.port + '/');

// ----------------------------------------------------------------------------
// response functions

function debug(action, msg) {
    console.log("%s DEBUG: %s - %s", iso8601(), action, msg);
}

function info(action, msg) {
    console.log("%s  INFO: %s - %s", iso8601(), action, msg);
}

function warn(action, msg) {
    console.log("%s  WARN : %s - %s", iso8601(), action, msg);
}

function error(action, msg) {
    console.log("%s ERROR: %s - %s", iso8601(), action, msg);
}

function fatal(action, msg) {
    console.log("%s FATAL: %s - %s", iso8601(), action, msg);
}

function op_add(req, parts, res) {
    var queuename = parts.query.queue || 'default';
    var msg = parts.query.msg;
    var id = parts.query.id || make_token();

    // return error if the message is undefined - empty messages are ok
    if ( typeof msg === 'undefined' ) {
        write_error(res, 1, 'Message is undefined');
        info('add', "message not specified");
        return;
    }

    ensure_queue(queuename);

    // make the actual add into a function so we can call it either when the
    // file has been written or immediately (if no filestore)
    var add_to_queue = function() {
        // add the message to the msg pile and add the 'id' to the queue
        queue[queuename].msg[id] = { 'id' : id, 'msg' : msg, 'inserted' : iso8601(), 'deliveries' : 0 };
        queue[queuename].queue.push(id);
        queue[queuename].seen++;

        info('add', "id=" + id);
        write_success(res, 'Message Added');
    };

    // see if we need to store it
    if ( cfg.store ) {
        var filename = cfg.filestore + "/" + id + ".txt";
        fs.writeFile(filename, msg, function (err) {
            if (err) {
                error('fs.writeFile', "couldn't save file: " + filename);
            }
            else {
                info('fs.writeFile', filename);
                add_to_queue();
            }
        });
    }
    else {
        // call the add immediately
        add_to_queue();
    }
}

function op_get(req, parts, res) {
    var queuename = parts.query.queue || 'default';
    ensure_queue(queuename);
    var q = queue[queuename];

    // keep looking through the message queue until you find one which hasn't
    // been deleted
    var id = null;
    while ( id === null && q.queue.length > 0 ) {
        id = q.queue.shift();
        // see if this message still exists
        if ( typeof q.msg[id] === 'undefined' ) {
            // nope, this has been deleted
            id = null;
        }
    }

    // either there were no messages or all the messages on the queue were
    // marked for deletion
    if ( id === null ) {
        write_success(res, 'No messages found');
        info("get", "no messages (queue=" + queuename + ")");
        return;
    }

    // generate a new token for this message and put onto the ack stash
    var token = make_token();

    // get the message from the msg stash
    var msg = q.msg[id];
    q.ack[token] = msg.id;

    // inc the number of deliveries
    msg.attempted = iso8601();
    msg.deliveries++;

    // create a timeout so we can store it on the message itself
    msg.timeout = setTimeout(function() {
        info("timeout", "id=" + id + ", token=" + token + ", deliveries=" + msg.deliveries);

        // put this message back on the queue
        delete msg.token; // no use for this anymore
        delete msg.timeout; // no use for this anymore
        delete q.ack[token];
        q.msg[id] = msg;
        q.queue.unshift(id);
    }, q.timeout * 1000);

    info("get", "id=" + msg.id + ", token=" + token + ", attempted=" + msg.deliveries);
    var data = {
        'id'         : msg.id,
        'msg'        : msg.msg,
        'token'      : token,
        'inserted'   : msg.inserted,
        'attempted'  : msg.attempted,
        'deliveries' : msg.deliveries
    };
    write_success( res, 'Message Returned', data );
}

function op_ack(req, parts, res) {
    var queuename = parts.query.queue || 'default';
    var token = parts.query.token;
    ensure_queue(queuename);
    var q = queue[queuename];

    if ( typeof token === 'undefined' ) {
        write_success(res, 'Token not specified (undefined)');
        info("ack", "error: token not specified (queue=" + queuename + ")");
        return;
    }

    // see if this token exists in the ack pile
    if ( typeof q.ack[token] === 'undefined' ) {
        // not there, so let's get out of here
        write_error(res, 6, 'Unknown Token: ' + token);
        info("ack", "error: unknown token (queue=" + queuename + ", token=" + token + ")");
        return;
    }

    // get the id from the token, and then get the message
    var id = q.ack[token];
    var msg = q.msg[id];

    // yes, it is in the ack pile, so delete it and remove it from the msg pile
    // 1) cancel the timeout
    clearTimeout( msg.timeout );

    // 2) remove from the ack pile
    delete q.ack[token];

    // 3) remove from the message pile
    delete q.msg[msg.id];

    // write result to client
    info("ack", "id=" + msg.id + ", token=" + token);
    write_success(res, 'Message Successfully Acked, Removed from Queue');

    // finally, remove the file
    if ( cfg.store ) {
        var filename = cfg.filestore + "/" + id + ".txt";
        fs.unlink(filename, function(err) {
            if (err) {
                error('fs.unlink', "couldn't unlink file: " + filename);
            }
            else {
                info('fs.unlink', filename);
            }
        });
    }
}

function op_del(req, parts, res) {
    var queuename = parts.query.queue || 'default';
    var id = parts.query.id;

    if ( typeof id === 'undefined' ) {
        info("del", "error: message id not specified");
        write_error(res, 7, 'Invalid ID (undefined)');
        return;
    }

    ensure_queue(queuename);
    var q = queue[queuename];

    // see if this id exists at all
    if ( q.msg[id] == null ) {
        // not there at all, so get out of here
        info("del", "error: unknown message (id=" + id + ")");
        write_error(res, 7, 'Unknown ID');
        return;
    }

    // delete from the msg stash and deal with this id when it appears on the
    // queue (or when an ack timer returns)
    var msg = q.msg[id];
    if ( msg.timeout ) {
        clearTimeout( msg.timeout );
    }
    delete q.msg[id];
    info("del", "id=" + id);
    write_success(res, 'Message Deleted');
}

function op_info(req, parts, res) {
    var queuename = parts.query.queue || 'default';
    ensure_queue(queuename);
    var q = queue[queuename];

    var data = {
        "name "      : queuename,
        "length"     : Object.keys(q.msg).length,
        "processing" : Object.keys(q.ack).length,
        "seen"       : q.seen
    };
    info('info', "queue=" + queuename);
    write_success(res, 'Info for queue ' + queuename, data);
}

function op_ping(req, parts, res) {
    info('ping', "pong");
    write_success(res, 'pong');
}

// ----------------------------------------------------------------------------
// utility functions

function read_config_file(filename) {
    // check if filename is defined
    if ( typeof filename === 'undefined' ) {
        console.log("No config specified");
        return { "port" : 8000 };
    }

    // load up the config file
    var cfg;
    try {
        cfg = JSON.parse(fs.readFileSync(filename));
    }
    catch (err) {
        console.warn("Couldn't read config file: " + err);
        process.exit(2)
    }

    // set some defaults if not set already
    cfg.port = cfg.port || '8000';
    sys.puts('Config: ' + sys.inspect(cfg));

    // see if they have defined a storage area
    if ( typeof cfg.filestore !== "undefined" ) {
        // check that this directory exists
        cfg.store = true;
        sys.puts("Reading outstanding files from '" + cfg.filestore + "/'");
    }

    // if we don't have a cluster, just make it blank
    if ( Array.isArray(cfg.nodes) ) {
        cfg.cluster = true;
    }

    return cfg;
}

function setup_queues (cfg) {
    // make sure the default queue is always there
    // queue['default'] = { 'msg' : {}, 'queue' : [], 'timeout' : 30, 'ack' : {}, 'deleted' : {} };
    ensure_queue('default', 30);

    // loop through any queues in the config file and set the timeouts
    for (queue_cfg in cfg['queues'] ) {
        ensure_queue( cfg['queues'][queue_cfg]['name'], cfg['queues'][queue_cfg]['timeout'] );
    }
}

function ensure_queue(queuename, timeout) {
    // set the default timeout (if none given)
    timeout = timeout || 30;

    // create the queue if not already defined
    if ( typeof queue[queuename] === 'undefined' ) {
        queue[queuename] = {
            'msg'     : {}, // the IDs of all the messages we know about
            'queue'   : [], // the actual queue itself (ie. the proper order of messages)
            'timeout' : timeout, // the timeout for this queue
            'ack'     : {}, // all the messages that are awaiting an 'ack'
            'deleted' : {}, // all messages that have been deleted (so they don't get returned)
            'seen'    : 0   // rolling count of all messages seen
        };
    }
}

function write_result(res, http_code, code, msg, data) {
    res.writeHead(http_code, {'Content-Type': 'application/json'});
    var resp = { 'status' : { 'ok' : (code === 0), 'code' : code, 'msg' : msg }, 'data' : data };
    res.end(JSON.stringify( resp ) + '\n' );
}

function write_success(res, msg, data) {
    write_result(res, 200, 0, msg, data);
}

function write_error(res, code, msg) {
    write_result(res, 200, code, msg);
}

function make_token () {
    // 64^8 = 281474976710656 (2 trillion)
    return random_string(8);
}

function random_string (length) {
    // 64 chars = 6 bits/char
    var chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-.';
    var str = '';
    while ( length > 0 ) {
        // get a random number from 0-63
        var rand = Math.floor(Math.random()*64);
        str = str + chars[rand];
        length--;
    }
    return str;
}

function iso8601() {
    var date = new Date();
    var str = date.getUTCFullYear() + '-' + pad(date.getUTCMonth(), 2) + '-' + pad(date.getUTCDay(), 2) + 'T';
    str = str + 'T';
    str = str + date.getUTCHours() + ':' + pad(date.getUTCMinutes(), 2) + ':' + pad(date.getUTCSeconds(), 2) + '.' + pad(date.getUTCMilliseconds(), 3) + 'Z';
    return str;
}

// this function pads the value to x significant figures
function pad (value, count) {
    var result = value + ''; // convert into a string first!
    while ( result.length < count ) {
        result = '0' + result;
    }
    return result;
}

// ----------------------------------------------------------------------------
