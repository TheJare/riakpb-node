# Riakpb-node

*Note: I never got to really use this module and consider it abandoned. If you are doing Node, Riak and
Protocol Buffers, you probably want this: [https://github.com/CrowdProcess/riak-pb](https://github.com/CrowdProcess/riak-pb)*

Self-contained implementation of a client for a Riak database using Protocol Buffers.
This is a binary protocol with persistent connection, so it should be faster than independent HTTP requests.

Sometimes it's nice to have a library with no dependencies.

Work in progress and not heavily tested, there may be broken functionality, utf8 shenanigans or unknown bugs.

## References

- Riak API: [http://wiki.basho.com/Client-Implementation-Guide.html](http://wiki.basho.com/Client-Implementation-Guide.html)
- Google's Protocol Buffers: [https://developers.google.com/protocol-buffers/docs/encoding](https://developers.google.com/protocol-buffers/docs/encoding)

## Credits & License
Copyright by Javier Arevalo in 2012.

- [http://www.iguanademos.com/Jare/](http://www.iguanademos.com/Jare/)
- [@TheJare](https://twitter.com/TheJare) on twitter

Licensed under the MIT license: [http://www.opensource.org/licenses/mit-license.php](http://www.opensource.org/licenses/mit-license.php)
