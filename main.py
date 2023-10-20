from abc import ABC, abstractmethod
import argparse
import logging
from time import sleep

from dotenv import load_dotenv
from sqlalchemy import and_, create_engine, select, String, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, Session
from spotipy import Spotify
from spotipy.oauth2 import SpotifyOAuth


logging.basicConfig(level=logging.INFO)
load_dotenv(".env")


SLEEPER = 3


class NoGenreException(Exception):
    pass


class Database:
    def __init__(self):
        class Base(DeclarativeBase):
            pass

        class PlaylistTrack(Base):
            __tablename__ = "playlist_track"
            __table_args__ = (
                UniqueConstraint("playlist_id", "track_id", name="_uc_pid_tid"),
            )

            id: Mapped[int] = mapped_column(primary_key=True)
            playlist_id: Mapped[str] = mapped_column(String(22))
            track_id: Mapped[str] = mapped_column(String(22))

        class GenrelessTrack(Base):
            __tablename__ = "genreless_track"
            __table_args__ = (UniqueConstraint("track_id", name="_uc_tid"),)

            id: Mapped[int] = mapped_column(primary_key=True)
            track_id: Mapped[str] = mapped_column(String(22))

        self.engine = create_engine("sqlite+pysqlite:///playlists.sqlite")
        self.Base = Base
        self.PlaylistTrack = PlaylistTrack
        self.GenrelessTrack = GenrelessTrack
        self.__setup()

    def __setup(self):
        self.Base.metadata.create_all(self.engine)

    def record_playlist_track(self, playlist_id, track_id):
        playlist_track = self.PlaylistTrack(playlist_id=playlist_id, track_id=track_id)

        with Session(self.engine) as session:
            try:
                session.add(playlist_track)
                session.commit()
            except:
                pass

    def check_playlist_track_exists(self, playlist_id, track_id):
        stmt = select(self.PlaylistTrack).where(
            and_(
                self.PlaylistTrack.playlist_id == playlist_id,
                self.PlaylistTrack.track_id == track_id,
            )
        )

        with Session(self.engine) as session:
            result = session.execute(stmt).first()

        if result is None:
            return False

        return True

    def record_genreless_track(self, track_id):
        genreless_track = self.GenrelessTrack(track_id=track_id)

        with Session(self.engine) as session:
            try:
                session.add(genreless_track)
                session.commit()
            except:
                pass


class SpotifyCache(ABC):
    def __init__(self, callback=lambda: None):
        self._cache = dict()
        self._callback = callback

    def set_connection(self, sp, user_id):
        self.sp = sp
        self._user_id = user_id

    def keys(self):
        return self._cache.keys()

    def __getitem__(self, _key):
        if _key not in self._cache:
            self._fetch_item(_key)

        return self._cache[_key]

    def __setitem__(self, _key, _value):
        self._cache[_key] = _value

    @abstractmethod
    def _fetch_item(self, _key):
        raise NotImplementedError


class PlaylistCache(SpotifyCache):
    def _fetch_item(self, _key):
        msg = f"PlaylistCache '{_key}' not found, creating..."
        logging.info(msg)
        pl = self.sp.user_playlist_create(self._user_id, _key, public=False)
        pl = Playlist(pl, self.sp, self._callback)
        self._cache[_key] = pl
        sleep(SLEEPER)


class ArtistCache(SpotifyCache):
    def _fetch_item(self, _key):
        msg = f"ArtistCache '{_key}' not found, fetching..."
        logging.debug(msg)
        artist = self.sp.artist(_key)
        artist = Artist(artist)
        self._cache[_key] = artist
        sleep(SLEEPER)


class AlbumCache(SpotifyCache):
    def _fetch_item(self, _key):
        msg = f"AlbumCache '{_key}' not found, fetching..."
        logging.debug(msg)
        album = self.sp.album(_key)
        album = Album(album)
        self._cache[_key] = album
        sleep(SLEEPER)


class SpotifyURNMixin:
    @property
    def urn(self):
        return f"spotify:{self.urn_type}:{self.id_}"


class Artist(SpotifyURNMixin):
    def __init__(self, api_reponse_item):
        self.urn_type = "artist"
        self.id_ = api_reponse_item["id"]
        self.name = api_reponse_item["name"]
        self.genres = api_reponse_item["genres"]


class Album(SpotifyURNMixin):
    def __init__(self, api_reponse_item):
        self.urn_type = "album"
        self.id_ = api_reponse_item["id"]
        self.name = api_reponse_item["name"]
        self.genres = api_reponse_item["genres"]


class Track(SpotifyURNMixin):
    def __init__(self, artist_cache, album_cache, api_response_item):
        self.urn_type = "track"
        self.artist_cache = artist_cache
        self.album_cache = album_cache

        self.id_ = api_response_item["track"]["id"].strip()
        self.title = api_response_item["track"]["name"].strip()

        artist_id = api_response_item["track"]["artists"][0]["id"].strip()
        self.artist = self.__set_artist(artist_id)

        album_id = api_response_item["track"]["album"]["id"].strip()
        self.album = self.__set_album(album_id)

        self.genres = self.__set_genres()

    def __str__(self):
        return f'"{self.title}" by {self.artist.name}'

    def __set_artist(self, id_):
        artist = self.artist_cache[id_]
        return artist

    def __set_album(self, id_):
        album = self.album_cache[id_]
        return album

    def __set_genres(self):
        if self.album.genres:
            return self.album.genres

        return self.artist.genres


class Playlist(SpotifyURNMixin):
    def __init__(self, api_response_item, sp, flush_callback=lambda: None):
        self.urn_type = "playlist"
        self.id_ = api_response_item["id"].strip()
        self.name = api_response_item["name"].strip()
        self.sp = sp
        self.tracks_to_add = []
        self.flush_callback = flush_callback

    def add_track(self, track):
        self.tracks_to_add.append(track)

        if len(self.tracks_to_add) == 100:
            logging.info(f"Max tracks for playlist {self.name}, flushing...")
            self.flush()
            self.tracks_to_add = []

    def flush(self):
        if len(self.tracks_to_add):
            msg = f"Flushing tracks for playlist: {self.name} ({self.id_})"
            logging.debug(msg)
            tracks_to_add = [t.urn for t in self.tracks_to_add]
            self.sp.playlist_add_items(self.id_, tracks_to_add)
            self.flush_callback(self.id_, self.tracks_to_add)
            sleep(SLEEPER)

    def __repr__(self):
        return f"<Playlist id={self.id_} name={self.name}>"


class ListMaker:
    def __init__(self, username):
        self.scope = ",".join(
            [
                "user-library-read",
                "playlist-modify-private",
                "playlist-read-private",
            ]
        )
        self.playlist_prefix = "Liked Songs:"
        self.username = username
        self.database = Database()
        self.playlist_cache = PlaylistCache(self.__flush_callback)
        self.artist_cache = ArtistCache()
        self.album_cache = AlbumCache()

    def connect(self):
        self.sp = Spotify(auth_manager=SpotifyOAuth(scope=self.scope))
        self.user_id = self.sp.me()["id"]
        self.playlist_cache.set_connection(self.sp, self.user_id)
        self.artist_cache.set_connection(self.sp, self.user_id)
        self.album_cache.set_connection(self.sp, self.user_id)

    def build_playlists(self):
        self.__generate_liked_playlists_map()
        self.__parse_liked_songs()
        self.__flush()

    def __generator(self, callable):
        offset = 0

        while True:
            results = callable(limit=50, offset=offset)

            if len(results["items"]) == 0:
                break

            yield from results["items"]

            offset = offset + 50
            sleep(SLEEPER)

    def __generate_liked_playlists_map(self):
        logging.debug("Generating existing genre playlist map")

        for pl in self.__generator(self.sp.current_user_playlists):
            pl = Playlist(pl, self.sp, self.__flush_callback)

            if pl.name.strip().startswith(self.playlist_prefix):
                self.playlist_cache[pl.name] = pl

        msg = f"Playlist map generated, {len(self.playlist_cache.keys())} found"
        logging.info(msg)

    def __parse_liked_songs(self):
        for t in self.__generator(self.sp.current_user_saved_tracks):
            t = Track(self.artist_cache, self.album_cache, t)

            try:
                self.__add_track_to_playlists(t)
            except NoGenreException:
                logging.info(f"NoGenreException: {t}")
                self.database.record_genreless_track(t.id_)

    def __add_track_to_playlists(self, t):
        if not t.genres:
            raise NoGenreException()

        for genre in t.genres:
            playlist_name = f"{self.playlist_prefix} {genre}"
            pl = self.playlist_cache[playlist_name]

            if not self.database.check_playlist_track_exists(pl.id_, t.id_):
                pl.add_track(t)

    def __flush(self):
        for _, pl in self.playlist_cache.items():
            pl.flush()

    def __flush_callback(self, playlist_id, tracks):
        for t in tracks:
            self.database.record_playlist_track(playlist_id, t.id_)


def get_args():
    parser = argparse.ArgumentParser(
        description="Convert your Liked Songs to playlists by genre, mood, and more."
    )
    parser.add_argument("-u", "--username", required=True, help="Spotify username")

    return parser.parse_args()


def main():
    args = get_args()
    lm = ListMaker(args.username)
    lm.connect()
    lm.build_playlists()


if __name__ == "__main__":
    main()
