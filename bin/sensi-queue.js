#!//usr/local/bin/node
// ----------------------------------------------------------------------------
//
// sensi-queue.js is a queue infrastructure service. This simple queue provides
// the ability to add, get and ack messages but also provides other useful
// information such as the time the message was added and the number of
// attempted deliveries. It has a default queue but you can use as many queues
// as you require which are automatically created.
//
// ----------------------------------------------------------------------------
//
// Sensi - set of 'Infrastructure as a Service' utilities which run on NodeJS.
// Copyright (c) 2010 by Andrew Chilton (chilts@appsattic.com).
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
var cfg = read_config_file( process.argv[2] || '/etc/sensi/sensi-queue.json');

// side effects to set things in 'queue'
setup_queues( cfg );

// create the webserver
http.createServer(function (req, res) {
    sys.puts('- START ------------------------------------------------------------------------');
    // get the different parts of the URL
    var parts = url.parse( req.url, true );
    if ( parts.query == null )
        parts.query = {};

    sys.puts('method  = ' + req.method);
    sys.puts('path    = ' + parts.pathname);
    sys.puts('queue   = ' + JSON.stringify(queue));
    sys.puts('');

    switch ( parts.pathname ) {
    case '/add':
        add(req, parts, res);
        break;

    case '/get':
        get(req, parts, res);
        break;

    case '/ack':
        ack(req, parts, res);
        break;

    case '/del':
        del(req, parts, res);
        break;

    default:
        return_result(res, 404, 404, 'Not Found', {});
        break;
    }

    sys.puts('');
    sys.puts('queue   = ' + JSON.stringify(queue));
    sys.puts('- END --------------------------------------------------------------------------');
}).listen(cfg.port);

sys.puts('Server running at http://127.0.0.1:' + cfg.port + '/');

// ----------------------------------------------------------------------------
// response functions

function add (req, parts, res) {
    var queuename = make_queuename(parts.query.queue);
    var msg = parts.query.msg;
    var id = parts.query.id || make_token();

    // return error if the message is undefined (an empty msg is ok)
    if ( typeof msg == 'undefined' ) {
        return_error(res, 1, 'Message is undefined');
        return;
    }

    ensure_queue(queuename);

    // add the message to the msg pile and add the 'id' to the queue
    queue[queuename].msg[id] = { 'id' : id, 'text' : msg, 'inserted' : iso8601(), 'delivered' : 0 };
    queue[queuename].queue.push(id);

    return_result(res, 200, 0, 'Message Added', {});
}

function get (req, parts, res) {
    var queuename = make_queuename(parts.query.queue);

    // if there is no queue for this at all, bail
    if ( typeof queue[queuename] == 'undefined' ) {
        return_error(res, 1, 'No queue of that name found');
        return;
    }

    // if there are no messages on the queue, bail
    if ( queue[queuename].queue.length == 0 ) {
        return_error(res, 2, 'No messages found');
        return;
    }

    // keep looking through the message queue until you find one which hasn't
    // been deleted
    var id = null;
    while ( id == null && queue[queuename].queue.length > 0 ) {
        id = queue[queuename].queue.shift();
        // see if this message is deleted
        if ( queue[queuename].deleted[id] == 1 ) {
            // delete all reference to this id
            delete queue[queuename].deleted[id];
            id = null;
        }
    }

    // all the messages on the queue were marked for deletion
    if ( id == null ) {
        return_error(res, 3, 'No messages found');
        return;
    }

    // get the message and inc the number of times it has been delivered
    var msg = queue[queuename].msg[id];
    msg.delivered++;

    // generate a new token for this message and remember it
    var token = make_token();

    var timeout = setTimeout(function() {
        sys.puts('- TIMEOUT ----------------------------------------------------------------------');
        sys.puts('queue   = ' + JSON.stringify(queue));
        sys.puts('');
        sys.puts('Message (ack=' + token + ') timed out');

        // put this message back on the queue
        queue[queuename].queue.unshift(id);
        delete msg.token; // no use for this anymore
        delete msg.timeout; // no use for this anymore
        delete queue[queuename].ack[token];

        sys.puts('');
        sys.puts('queue   = ' + JSON.stringify(queue));
        sys.puts('- END --------------------------------------------------------------------------');
    }, queue[queuename].timeout * 1000);

    // put the id in the ack pile and save this timeout and token on the message
    queue[queuename].ack[token] = id;
    msg.token = token;
    msg.timeout = timeout;

    return_result(res, 200, 0, 'Message Returned', { 'id' : msg.id, 'text' : msg.text, 'token' : token, 'inserted' : msg.inserted, 'delivered' : msg.delivered });
}

function ack (req, parts, res) {
    var queuename = make_queuename(parts.query.queue);
    var token = parts.query.token;

    // see if this token exists in the ack pile
    if ( queue[queuename].ack[token] == null ) {
        // not there, so let's get out of here
        res.writeHead(200, {'Content-Type': 'text/json'});
        var result = { 'status' : { 'code' : 100, 'msg' : 'Token not known' } };
        res.end(JSON.stringify( result ) + '\n' );
        return;
    }

    // yes, it is in the ack pile, so delete it and remove it from the msg pile
    // 1) cancel the timeout
    var id = queue[queuename].ack[token];
    // var msg = queue[queuename].msg[id];
    clearTimeout( queue[queuename].msg[id].timeout );

    // 2) remove from the ack pile
    delete queue[queuename].ack[token];

    // 3) remove from the message pile
    delete queue[queuename].msg[id];

    // write result to client
    return_result(res, 200, 0, 'Message Successfully Acked, Removed from Queue', {});
}

function del (req, parts, res) {
    var queuename = make_queuename(parts.query.queue);
    var id = parts.query.id;

    // see if this id exists at all
    if ( queue[queuename].msg[id] == null ) {
        // not there, so let's get out of here
        res.writeHead(200, {'Content-Type': 'text/json'});
        var result = { 'status' : { 'code' : 100, 'msg' : 'Message ID not known' } };
        res.end(JSON.stringify( result ) + '\n' );
        return;
    }

    // yes, it is known about, so firstly check if we need to cancel a timeout
    var msg = queue[queuename].msg[id];

    // If this message has a token, then it should be removed from the ack
    // pile, the timeout cancelled and not put onto the deleted pile (since it
    // is no longer in the queue).
    if ( msg.token ) {
        delete queue[queuename].ack[msg.token];
        clearTimeout(msg.timeout);
        // no need of these things anymore
        delete msg.token;
        delete msg.timeout;
    }
    else {
        // since this msg is still in the queue (ie. it has no timeout), then
        // it needs to be remembered on the deleted queue so we don't return it
        queue[queuename].deleted[id] = 1;
    }

    // finally, delete it from the msg pile
    delete queue[queuename].msg[id];

    // write result to client
    return_result(res, 200, 0, 'Message Successfully Deleted', {});
}

// ----------------------------------------------------------------------------
// utility functions

function read_config_file(filename) {
    // load up the config file
    try {
        var cfg = JSON.parse(fs.readFileSync(filename));
    }
    catch (err) {
        var cfg = {};
    }

    // set some defaults if not set already
    cfg.port = cfg.port || '8000';
    sys.puts(JSON.stringify(cfg));

    return cfg;
}

function setup_queues (cfg) {
    // make sure the default queue is always there
    queue['default'] = { 'msg' : {}, 'queue' : [], 'timeout' : 30, 'ack' : {}, 'deleted' : {} };

    // loop through any queues in the config file and set the timeouts
    for (queue_cfg in cfg['queues'] ) {
        // Each queue has:
        // * msg     - a hash containing the messages themselves
        // * queue   - an array showing the order of the messages
        // * timeout - the default timeout for this queue
        // * ack     - a hash showing which have been delivered and awaiting an ack
        // * deleted - a hash showing which have been deleted
        queue[cfg['queues'][queue_cfg]['name']] = {
            'msg'     : {},
            'queue'   : [],
            'timeout' : cfg['queues'][queue_cfg]['timeout'] || 30,
            'ack'     : {},
            'deleted' : {}
        };
    }
}

function ensure_queue(queuename) {
    if ( typeof queue[queuename] == 'undefined' ) {
        queue[queuename] = { 'msg' : {}, 'queue' : [], 'timeout' : 30, 'ack' : {}, 'deleted' : {} };
    }
}

function return_result (res, http_code, code, msg, result) {
    res.writeHead(http_code, {'Content-Type': 'application/json'});
    var resp = { 'status' : { 'code' : code, 'msg' : msg }, 'result' : result };
    res.end(JSON.stringify( resp ) + '\n' );
}

function return_error (res, code, msg) {
    return_result(res, 200, code, msg, {});
}

function make_queuename (q) {
    return q ? q : 'default';
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

function iso8601 () {
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
