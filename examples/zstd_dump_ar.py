"""
ETG API provides hotel's static data dump in .zstd format.
You can find more about the dump structure and the format in our documentation - https://docs.emergingtravel.com/#0b55c99a-7ef0-4a18-bbfe-fd1bdf35d08e

Please note that uncompressed data could be more than 20GB.

Below is an example of how to handle such large archive.

For decompression, we will use the zstandard library which you can install using the command
> pip install zstandard

The script takes the path to the archive file,
splits the whole file by 16MB chunks,
extracts objects line by line (each line contains one hotel in JSON format),
and converts them into Python dicts which you can use in your inner logic.
"""

import json
from io import TextIOWrapper

from zstandard import ZstdDecompressor
import psycopg2


def parse_dump(filename: str) -> None:
    """
    The sample of function that can parse a big zstd dump.
    :param filename: path to a zstd archive
    """

    conn = psycopg2.connect(
        dbname="jadwelny_database",
        user="postgres",
        password="J@dwelny2023",
        host="localhost",
        port="5432"
    )

    cur = conn.cursor()

    with open(filename, "rb") as fh:
        # make decompressor
        dctx = ZstdDecompressor()
        with dctx.stream_reader(fh) as reader:
            wrapper = TextIOWrapper(reader, "utf-8")
            previous_line = ""
            while True:
                # we will read the file by chunks of 16M UTF-8 characters
                raw_data = wrapper.read(16_000_000)
                if not raw_data:
                    break

                # all JSON files split by the new line char "\n"
                # try to read one by one
                lines = raw_data.split("\n")
                for i, line in enumerate(lines[:-1]):
                    if i == 0:
                        line = previous_line + line
                    hotel_data = json.loads(line)
                    # do stuff with the hotel

                    cur.execute(
                        "SELECT COUNT(*) FROM hotel_dump_ar WHERE hotel_id = %s", (hotel_data['id'],))
                    if cur.fetchone()[0] > 0:
                        print(
                            f"{i} - Hotel {hotel_data['id']} found in database. Updating...")

                        # Update query for existing hotel
                        hotel_data_str = json.dumps(hotel_data)
                        update_query = 'UPDATE hotel_dump_ar SET dump = %s, "updatedAt" = %s WHERE hotel_id = %s'
                        try:
                            cur.execute(
                                update_query, (hotel_data_str, "2023-09-01 21:31:07.595+04", hotel_data['id']))
                            conn.commit()
                        except Exception as e:
                            conn.rollback()
                            print(
                                f"Failed to update hotel {hotel_data['id']}. Error: {e}")

                        continue

                    # ANAS's code
                    hotel_data_str = json.dumps(hotel_data)

                    insert_query = 'INSERT INTO hotel_dump_ar (hotel_id, dump, "createdAt", "updatedAt") VALUES (%s, %s, %s, %s)'
                    try:
                        cur.execute(
                            insert_query, (hotel_data['id'], hotel_data_str, "2023-09-01 21:31:07.595+04", "2023-09-01 21:31:07.595+04"))
                        conn.commit()
                    except Exception as e:
                        conn.rollback()
                        return f"Failed to insert hotel {hotel_data['id']}. Error: {e}"
                    
                    print(f"current hotel is {hotel_data['name']}")
                previous_line = lines[-1]


if __name__ == "__main__":
    parse_dump("partner_feed_ar.json.zst")
