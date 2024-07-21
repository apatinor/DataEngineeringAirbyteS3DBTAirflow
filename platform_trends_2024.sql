-- models/platform_trends_2024.sql
{{ config(materialized="table", alias="platform_trends_2024") }}

with
    base as (
        select
            "Track" as track,
            "Album_Name" as album_name,
            "Artist" as artist,
            "Release_Date"::date as release_date,
            "ISRC" as isrc,
            nullif(replace("All_Time_Rank", ',', ''), '')::int as all_time_rank,
            "Track_Score"::numeric as track_score,
            nullif(replace("Spotify_Streams", ',', ''), '')::bigint as spotify_streams,
            nullif(replace("Spotify_Playlist_Count", ',', ''), '')::int
            as spotify_playlist_count,
            nullif(replace("Spotify_Playlist_Reach", ',', ''), '')::bigint
            as spotify_playlist_reach,
            nullif(replace("Spotify_Popularity", ',', ''), '')::int
            as spotify_popularity,
            nullif(replace("YouTube_Views", ',', ''), '')::bigint as youtube_views,
            nullif(replace("YouTube_Likes", ',', ''), '')::bigint as youtube_likes,
            nullif(replace("TikTok_Posts", ',', ''), '')::bigint as tiktok_posts,
            nullif(replace("TikTok_Likes", ',', ''), '')::bigint as tiktok_likes,
            nullif(replace("TikTok_Views", ',', ''), '')::bigint as tiktok_views,
            nullif(replace("YouTube_Playlist_Reach", ',', ''), '')::bigint
            as youtube_playlist_reach,
            nullif(replace("Apple_Music_Playlist_Count", ',', ''), '')::int
            as apple_music_playlist_count,
            nullif(replace("AirPlay_Spins", ',', ''), '')::bigint as airplay_spins,
            nullif(replace("SiriusXM_Spins", ',', ''), '')::bigint as siriusxm_spins,
            nullif(replace("Deezer_Playlist_Count", ',', ''), '')::int
            as deezer_playlist_count,
            nullif(replace("Deezer_Playlist_Reach", ',', ''), '')::bigint
            as deezer_playlist_reach,
            nullif(replace("Amazon_Playlist_Count", ',', ''), '')::int
            as amazon_playlist_count,
            nullif(replace("Pandora_Streams", ',', ''), '')::bigint as pandora_streams,
            nullif(replace("Pandora_Track_Stations", ',', ''), '')::int
            as pandora_track_stations,
            nullif(replace("Soundcloud_Streams", ',', ''), '')::bigint
            as soundcloud_streams,
            nullif(replace("Shazam_Counts", ',', ''), '')::bigint as shazam_counts,
            nullif(replace("TIDAL_Popularity", ',', ''), '')::int as tidal_popularity,
            "Explicit_Track"::boolean as explicit_track,
            now() as last_update
        from "postgresdb_4izx"."public"."s3_spotify_spotify_trends_2024"
    )

select *
from base
