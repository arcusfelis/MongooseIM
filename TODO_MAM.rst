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


