=====================
TODO list for mod_mam
=====================

Packet transformation before archiving
--------------------------------------

Q: Should ``<archived/>`` tags are saved into an archive?

A: Archiving servers supporting MAM MUST strip any existing ``<archived/>`` element
with a 'by' attribute equal to an archive that they provide.
See ``strip_archived`` test case.

Packet filtration
-----------------

Q: Which MUC-messages should be saved into an archive?

Q: Should we archive offline messages?
A: Yes, we should.

A message addressed to an unknown user MUST be dropped.
Use ``ejabberd_auth:is_user_exists(LUser, LServer)`` to distinguish.

We can use ``ejabberd_auth_anonymous:anonymous_user_exist(User, Server)``
or ``ejabberd_auth_anonymous:is_user_exists(User, Server)``
to detect anonymous users.

Do not archive MUC-messages using ``mod_mam`` (use ``mod_mam_muc`` only).

API to configure MUC archives.
------------------------------

- It looks like it will be available from Erlang only.
- Are prefs from XEP-0313 applicable in this case?


Hooks
-----

Q: Which function does delete information about a room from the DB?
A: It is ``mod_muc:forget_room/2``. It has no hooks, so we should add one.

We can use the same hooks for incoming messages as in ``mod_offline``.

Hooks ``user_receive_packet`` and ``filter_packet`` are different.
First is executed after routing for connected user, the second is
called before.


Caching
-------

Results of ``ejabberd_auth:is_user_exists/2`` SHOULD be cached.

Unique IDs
----------

We can use ``{now(), node()}``.
Each node can have a short id.

``filter_packet`` is called on the sender's node of the cluster (at least in
MongooseIM). 
Can also use ``{from(), now(), to()}`` (this does not work, because anyone
can call ``ejabber_router:route/*``).

Tests
-----

- Check dates with escalus.


Maximum length of JID
---------------------

From http://xmpp.org/extensions/xep-0029.html
node (256 bytes)@domain (255 bytes, 7-bit encoding)/resource (256 bytes)
256+1+255+1+256=769 (bytes)

From http://www.jabber.org/ietf/draft-ietf-xmpp-core-20.html
Each allowable portion of a JID (node identifier, domain identifier, and 
resource identifier) MUST NOT be more than 1023 bytes in length, resulting in 
a maximum total size (including the '@' and '/' separators) of 3071 bytes.


DDoS protection
---------------

Limiting the number of XMPP resource identifiers allowed to an account at any
one time. This may help to prevent a rogue account from creating an unlimited
number of sessions and therefore exhausting the resources of the server's
session manager.

http://xmpp.org/extensions/xep-0205.html
