#!//usr/local/bin/node
// ----------------------------------------------------------------------------
//
// Sensi - set of 'Infrastructure as a Service' utilities which run on NodeJS.
// Copyright (c) 2010 by Andrew Chilton (chilts@appsattic.com).
//
// ----------------------------------------------------------------------------
//
// sensi-sq.js is a queue infrastructure service. This simple queue provides
// the ability to add, get and ack messages but also provides other useful
// information such as the time the message was added and the number of
// attempted deliveries. It has a default queue but you can use as many queues
// as you require which are automatically created.
//
// If you require a distributed and therefore redundant queue, please look at
// sensi-dq.js.
//
// ----------------------------------------------------------------------------
//
// This file is part of Sensi.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the Affero GNU General Public License as published by the
// Free Software Foundation, either version 3 of the License, or (at your
// option) any later version.
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
var ack_list = {};
var cfg = {};

// read the config file (if it exists)
var cfg = JSON.parse(fs.readFileSync('./sensi-sq.json'));
sys.puts('cfg=' + JSON.stringify(cfg));
cfg['port'] = cfg['port'] || '8000';

// make sure the default queue is always there
queue['default'] = { 'queue' : [], 'timeout' : 30 };

// loop through any queues in the config file and set the timeouts
for (queue_cfg in cfg['queues'] ) {
    // if no timeout specified, set it to be 30 secs
    queue[cfg['queues'][queue_cfg]['name']] = {
        'queue' : [],
        'timeout' : cfg['queues'][queue_cfg]['timeout'] || 30
    };
}

// show what the queues now looks like
sys.puts('queue   = ' + JSON.stringify(queue));

http.createServer(function (req, res) {
    sys.puts('- START ------------------------------------------------------------------------');
    // get the different parts of the URL
    var parts = url.parse( req.url, true );
    if ( parts.query == null )
        parts.query = {};

    sys.puts('method  = ' + req.method);
    sys.puts('path    = ' + parts.pathname);
    sys.puts('queue   = ' + JSON.stringify(queue));
    sys.puts('ack_list = ' + JSON.stringify(ack_list));
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

    default:
        return_result(res, 404, 404, 'Not Found', {});
        break;
    }

    sys.puts('');
    sys.puts('queue   = ' + JSON.stringify(queue));
    sys.puts('ack_list = ' + JSON.stringify(ack_list));
    sys.puts('- END --------------------------------------------------------------------------');
}).listen(cfg['port']);

sys.puts('Server running at http://127.0.0.1:' + cfg['port'] + '/');

// ----------------------------------------------------------------------------
// response functions

function add (req, parts, res) {
    sys.puts('Got an /add');

    var queuename = make_queuename(parts.query.queue);
    var msg = parts.query.msg;
    var id = parts.query.id;
    sys.puts('Message = ' + msg);
    sys.puts('Queue   = ' + queuename);

    // return error if the message is undefined (an empty msg is ok)
    if ( typeof msg == 'undefined' ) {
        return_error(res, 1, 'Message is undefined');
        return;
    }

    // if there is no queue of that name yet, make one
    if ( typeof queue[queuename]['queue'] == 'undefined' ) {
        queue[queuename]['queue'] = [];
    }

    // 'id' is optional, but create one if we need to
    id = id || make_token();

    // add the message to the queue
    queue[queuename]['queue'].push({ 'id' : id, 'text' : msg, 'inserted' : iso8601(), 'delivered' : 0 });
    sys.puts('msg = ' + JSON.stringify(msg));

    return_result(res, 200, 0, 'Message Added', {});
}

function get (req, parts, res) {
    sys.puts('Got a /get');

    sys.puts('Queue = ' + parts.query.queue);
    var queuename = make_queuename(parts.query.queue);
    sys.puts('Queue = ' + queuename);

    // if there is no queue for this at all, bail
    if ( typeof queue[queuename] == 'undefined' ) {
        return_error(res, 1, 'No queue of that name found');
        return;
    }

    // if there are no messages on the queue, bail
    if ( queue[queuename]['queue'].length == 0 ) {
        return_error(res, 2, 'No messages found');
        return;
    }

    // get the message and increment the number of times it has been delivered
    var msg = queue[queuename]['queue'].shift();
    msg.delivered++;

    // generate a new token for this message and remember it
    var token = make_token();
    sys.puts('token=' + token);

    var timeout = setTimeout(function() {
        sys.puts('- TIMEOUT ----------------------------------------------------------------------');
        sys.puts('queue   = ' + JSON.stringify(queue));
        sys.puts('ack_list = ' + JSON.stringify(ack_list));
        sys.puts('');
        sys.puts('Message (ack=' + token + ') timed out');

        // put this message back on the queue
        queue[queuename]['queue'].unshift(msg);
        delete ack_list[queuename][token];

        sys.puts('');
        sys.puts('queue   = ' + JSON.stringify(queue));
        sys.puts('ack_list = ' + JSON.stringify(ack_list));
        sys.puts('- END --------------------------------------------------------------------------');
    }, queue[queuename]['timeout'] * 1000);

    // now that we have everything, put it on the ack_list
    if ( typeof ack_list[queuename] == 'undefined' ) {
        ack_list[queuename] = {};
    }
    ack_list[queuename][token] = { 'msg' : msg, 'timeout' : timeout };

    // ToDo: replace this with return_result
    return_result(res, 200, 0, 'Message Returned', { 'id' : msg.id, 'text' : msg.text, 'token' : token, 'inserted' : msg.inserted, 'delivered' : msg.delivered });
}

function ack (req, parts, res) {
    sys.puts('Got an /ack');

    var queuename = make_queuename(parts.query.queue);
    var token = parts.query.token;

    sys.puts('queuename = ' + queuename);
    sys.puts('token     = ' + token);
    sys.puts('ack_list  = ' + JSON.stringify(ack_list));

    if ( typeof ack_list[queuename] == 'undefined' ) {
        ack_list[queuename] = {};
    }

    // see if this token exists in the ack_list
    if ( ack_list[queuename][token] == null ) {
        // not there, so let's get out of here
        res.writeHead(200, {'Content-Type': 'text/json'});
        var result = { 'status' : { 'code' : 100, 'msg' : 'Token not known' } };
        res.end(JSON.stringify( result ) + '\n' );
        return;
    }

    // yes, it is in the ack_list, so just delete it
    var timeout = ack_list[queuename][token].timeout;
    clearTimeout(timeout);
    delete ack_list[queuename][token];

    // write result to client
    return_result(res, 200, 0, 'Message Successfully Acked, Removed from Queue', {});
}

// ----------------------------------------------------------------------------
// utility functions

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
