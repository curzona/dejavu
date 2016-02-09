from __future__ import absolute_import
from itertools import izip_longest
import Queue

from dejavu.database import Database


class MemoryDatabase(Database):
    type = "memory"

    # fields
    FIELD_FINGERPRINTED = "fingerprinted"

    def __init__(self, **options):
        super(MemoryDatabase, self).__init__()
        self._options = options

    def after_fork(self):
        # Clear the cursor cache, we don't want any stale connections from
        # the previous process.
        pass

    def setup(self):
        """
        Creates any non-existing tables required for dejavu to function.

        This also removes all songs that have been added but have no
        fingerprints associated with them.
        """
        self.fingerprints = {}
        self.songs = {}
        self._song_id = 1;

    def empty(self):
        """
        Drops tables created by dejavu and then creates them again.

        .. warning:
            This will result in a loss of data
        """
        self.fingerprints = None
        self.songs = None

        self.setup()

    def delete_unfingerprinted_songs(self):
        """
        Removes all songs that have no fingerprints associated with them.
        """
        raise NotImplemented

    def get_num_songs(self):
        """
        Returns number of songs the database has fingerprinted.
        """
        raise NotImplemented

    def get_num_fingerprints(self):
        """
        Returns number of fingerprints the database has fingerprinted.
        """
        raise NotImplemented

    def set_song_fingerprinted(self, sid):
        """
        Set the fingerprinted flag to TRUE (1) once a song has been completely
        fingerprinted in the database.
        """
       
        self.songs[sid][MemoryDatabase.FIELD_FINGERPRINTED] = True

    def get_songs(self):
        """
        Return songs that have the fingerprinted flag set TRUE (1).
        """
        for k, v in self.songs.items():
            if v[MemoryDatabase.FIELD_FINGERPRINTED]:
                yield v

    def get_song_by_id(self, sid):
        """
        Returns song by its ID.
        """
        return self.songs[sid]

    def insert(self, hash, sid, offset):
        """
        Insert a (sha1, song_id, offset) row into database.
        """
        raise NotImplemented

    def insert_song(self, songname, file_hash):
        """
        Inserts song in the database and returns the ID of the inserted record.
        """
        song_id = self._song_id
        self._song_id += 1

        self.songs[song_id] = {
            Database.FIELD_SONG_ID: song_id,
            Database.FIELD_SONGNAME: songname,
            Database.FIELD_FILE_SHA1: file_hash
        }

        return song_id

    def query(self, hash):
        """
        Return all tuples associated with hash.

        If hash is None, returns all entries in the
        database (be careful with that one!).
        """
        raise NotImplemented

    def get_iterable_kv_pairs(self):
        """
        Returns all tuples in database.
        """
        return self.query(None)

    def insert_hashes(self, sid, hashes):
        """
        Insert series of hash => song_id, offset
        values into the database.
        """
        values = []
        for hash, offset in hashes:
            self.fingerprints[hash] = {
                Database.FIELD_HASH: hash,
                Database.FIELD_SONG_ID: sid,
                Database.FIELD_OFFSET: offset,
            }

    def return_matches(self, hashes):
        """
        Return the (song_id, offset_diff) tuples associated with
        a list of (sha1, sample_offset) values.
        """
        # Create a dictionary of hash => offset pairs for later lookups
        mapper = {}
        for hash, offset in hashes:
            mapper[hash] = offset

        # Get an iteratable of all the hashes we need
        values = mapper.keys()

        for value in values:
            if value in self.fingerprints:
                result = self.fingerprints[value]
                sid = result[Database.FIELD_SONG_ID]
                offset = result[Database.FIELD_OFFSET]
                hash = result[Database.FIELD_HASH]

                yield (sid, offset - mapper[hash])

    def __getstate__(self):
        return (self._options,)

    def __setstate__(self, state):
        self._options, = state
        #self.cursor = cursor_factory(**self._options)


def grouper(iterable, n, fillvalue=None):
    args = [iter(iterable)] * n
    return (filter(None, values) for values
            in izip_longest(fillvalue=fillvalue, *args))

