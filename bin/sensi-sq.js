#!//usr/local/bin/node
// ----------------------------------------------------------------------------
//
// Sensi - s set of Infrastructure As A Service utilities which run on NodeJS.
// Copyright © 2010 by Andrew Chilton (chilts@appsattic.com)
//
// ----------------------------------------------------------------------------
//
// sensi-sq.js is a queue infrastructure service. This simple queue provides
// the ability to add, get and ack messages but also provides other useful
// information such as the time the message was added and the number of
// attempted deliveries. It has a default queue but you can use as many queues
// as you require which are automatically created.
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
var sys = require('sys');
var http = require('http');
var url = require('url');

// global vars :)
var queue = {};
var ack_list = {};

http.createServer(function (req, res) {
    sys.puts('- START ------------------------------------------------------------------------');
    // get the different parts of the URL
    var parts = url.parse( req.url, true );
    if ( parts.query == null )
        parts.query = {};

    // ToDo: check req.method is what we expect: PUT, POST, GET, DELETE

    sys.puts('method  = ' + req.method);
    sys.puts('path    = ' + parts.pathname);
    sys.puts('queue   = ' + JSON.stringify(queue));
    sys.puts('ack_list = ' + JSON.stringify(ack_list));
    sys.puts('');

    if ( parts.pathname == '/add' ) {
        sys.puts('Got an /add');

        var queuename = make_queuename(parts.query.queue);
        var msg = parts.query.msg;
        sys.puts('Message = ' + msg);
        sys.puts('Queue   = ' + queuename);

        if ( typeof queue[queuename] == 'undefined' ) {
            queue[queuename] = [];
        }
        queue[queuename].push(msg);

        return_result(res, 200, 0, 'Message Added', {});
    }
    else if ( parts.pathname == '/get' ) {
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
        if ( queue[queuename].length == 0 ) {
            return_error(res, 2, 'No messages found');
            return;
        }

        // get the message
        var msg = queue[queuename].shift();

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
            queue[queuename].unshift(msg);
            delete ack_list[queuename][token];

            sys.puts('');
            sys.puts('queue   = ' + JSON.stringify(queue));
            sys.puts('ack_list = ' + JSON.stringify(ack_list));
            sys.puts('- END --------------------------------------------------------------------------');
        }, 10000);

        // now that we have everything, put it on the ack_list
        if ( typeof ack_list[queuename] == 'undefined' ) {
            ack_list[queuename] = {};
        }
        ack_list[queuename][token] = { 'msg' : msg, 'timeout' : timeout };

        // ToDo: replace this with return_result
        return_result(res, 200, 0, 'Message Returned', { 'msg' : msg, 'token' : token });
    }
    else if ( parts.pathname == '/ack' ) {
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
    else {
        return_result(res, 404, 404, 'Not Found', {});
    }
    sys.puts('');
    sys.puts('queue   = ' + JSON.stringify(queue));
    sys.puts('ack_list = ' + JSON.stringify(ack_list));
    sys.puts('- END --------------------------------------------------------------------------');
}).listen(8000);

sys.puts('Server running at http://127.0.0.1:8000/');

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
