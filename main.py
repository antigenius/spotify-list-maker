from abc import abstractmethod
import argparse
import logging
from time import sleep

from dotenv import load_dotenv
from sqlalchemy import create_engine
from spotipy import Spotify
from spotipy.oauth2 import SpotifyOAuth


logging.basicConfig(level=logging.DEBUG)
load_dotenv(".env")


SLEEPER = 3


class NoGenreException(Exception):
    pass


class SpotifyCache:
    def __init__(self):
        self.__cache = dict()

    def set_connection(self, sp, user_id):
        self.sp = sp
        self._user_id = user_id

    def keys(self):
        return self.__cache.keys()

    def __getitem__(self, __key):
        if __key not in self.__cache:
            self.__fetch_item(__key)

        return self.__cache[__key]

    def __setitem__(self, __key, __value):
        self.__cache[__key] = __value

    @abstractmethod
    def __fetch_item(self, __key):
        raise NotImplementedError


class PlaylistCache:
    def __fetch_item(self, __key):
        msg = f"PlaylistCache '{__key}' not found, creating..."
        logging.info(msg)
        pl = self.sp.user_playlist_create(self.user_id, __key, public=False)
        pl = Playlist(pl, self.sp)
        self.__cache[__key] = pl
        sleep(SLEEPER)


class ArtistCache(dict):
    def __fetch_item(self, __key):
        msg = f"ArtistCache '{__key}' not found, fetching..."
        logging.debug(msg)
        artist = self.sp.artist(__key)
        artist = Artist(artist)
        self.__cache[__key] = artist
        sleep(SLEEPER)


class AlbumCache(dict):
    def __fetch_item(self, __key):
        msg = f"AlbumCache '{__key}' not found, fetching..."
        logging.debug(msg)
        album = self.sp.album(__key)
        album = Album(album)
        self.__cache[__key] = album
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
    def __init__(self, api_response_item, sp):
        self.urn_type = "playlist"
        self.id_ = api_response_item["id"].strip()
        self.name = api_response_item["name"].strip()
        self.sp = sp
        self.tracks_to_add = []

    def add_track(self, track):
        self.tracks_to_add.append(track.urn)

        if len(self.tracks_to_add) == 100:
            logging.info(f"Max tracks for playlist {self.name}, flushing...")
            self.flush()
            self.tracks_to_add = []

    def flush(self):
        if len(self.tracks_to_add):
            msg = f"Flushing tracks for playlist: {self.name} ({self.id_})"
            logging.debug(msg)
            self.sp.playlist_add_items(self.id_, self.tracks_to_add)
            sleep(SLEEPER)

    def __del__(self):
        self.flush()

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
        self.playlist_cache = PlaylistCache()
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
            pl = Playlist(pl, self.sp)

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

    def __add_track_to_playlists(self, t):
        if not t.genres:
            raise NoGenreException()

        for genre in t.genres:
            playlist_name = f"{self.playlist_prefix} {genre}"
            pl = self.playlist_cache[playlist_name]
            pl.add_track(t)

    def __flush(self):
        for _, pl in self.playlist_cache.items():
            pl.flush()


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
