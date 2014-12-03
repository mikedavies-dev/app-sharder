/**

 The MIT License (MIT)

 Copyright (c) 2013 LifeWanted

 Permission is hereby granted, free of charge, to any person obtaining a copy of
 this software and associated documentation files (the "Software"), to deal in
 the Software without restriction, including without limitation the rights to
 use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 the Software, and to permit persons to whom the Software is furnished to do so,
 subject to the following conditions:

 The above copyright notice and this permission notice shall be included in all
 copies or substantial portions of the Software.

 THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

 https://github.com/LifeWanted/node-redis-shard/blob/master/lib/HashRing.js

 */

var crc32 = require( 'crc32' );

module.exports = (function(){
    const MAX_INT_32    = 0x7fffffff;
    const MAX_UINT_32   = 0xffffffff;

    function HashRing( nodes, replicas ){
        this.nodes      = [];
        this.replicas   = replicas || 128;
        this.ring       = {};
        this.sortedKeys = [];

        for( var i in nodes ){
            this.addNode( nodes[ i ] );
        }
    }

    var HashRingProto = HashRing.prototype;

    HashRingProto.addNode = function( node ){
        this.nodes.push( node );
        for( var i = 0; i < this.replicas; ++i ){
            var crckey = hash( node + ':' + i );
            this.ring[ crckey ] = node;
            this.sortedKeys.push( crckey );
        }
        this.sortedKeys.sort(function( a, b ){ return a - b; });
    };

    HashRingProto.getNode = function( key ){
        if( this.sortedKeys.length == 0 ){
            return null;
        }

        var crcKey  = hash( key );
        var idx     = bisect( this.sortedKeys, crcKey );
        idx = Math.min( idx, (this.replicas * this.nodes.length) - 1 );
        return this.ring[ this.sortedKeys[ idx ] ];
    };

    function hash( key ){
        var crcHash = parseInt( crc32( key ), 16 );
        if( crcHash > MAX_INT_32 ){
            return -(MAX_UINT_32 - crcHash + 1);
        }
        return crcHash;
    }

    function bisect( arr, val ){
        var len     = arr.length;
        var i       = Math.round( len / 2 ) || 1;
        var step    = Math.round( i / 2 )   || 1;

        while(
        (arr[ i ] <= val    && i != arr.length - 1) ||
        (arr[ i ] > val     && i !== 0)
            ){
            if( arr[ i ] > val && (i === 0 || arr[ i - 1 ] <= val) ){
                break;
            }

            if( arr[ i ] <= val ){
                i += step;
            }
            else {
                i -= step;
            }
            step = Math.round( step / 2 ) || 1;
        }

        i = arr[ i ] <= val ? i + 1 : i;
        return i;
    }

    return HashRing;
})();