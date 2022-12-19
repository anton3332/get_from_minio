import os
import argparse
from time import sleep
from minio import Minio


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-url", help="URL of server")
    parser.add_argument("-acc", help="Access key (login)")
    parser.add_argument("-secret", help="Secret key (password)")
    parser.add_argument("-bucket", help="Bucket name")
    parser.add_argument("-out_dir", help="Directory to download files to")
    parser.add_argument("-pause", type=float, help="Pause between attempts")
    args = parser.parse_args()

    existing_files = set()
    out_dir = args.out_dir
    if not os.path.isdir(out_dir):
        os.mkdir(out_dir)
    else:
        for root, dirs, files in os.walk(out_dir, True):
            existing_files.update(files)
            break

    client = Minio(args.url, access_key=args.acc, secret_key=args.secret)

    bucket_name = args.bucket

    while True:
        objects = client.list_objects(bucket_name)
        for obj in objects:
            if obj.object_name not in existing_files:
                print("Downloading " + obj.object_name)
                response = client.get_object(bucket_name, obj.object_name)
                try:
                    file_data = response.read()
                finally:
                    response.close()
                    response.release_conn()
                file_path = os.path.join(out_dir, obj.object_name)
                try:
                    with open(file_path, "wb") as f:
                        f.write(file_data)
                except:
                    if os.path.isfile(file_path):
                        os.remove(file_path)
                    raise
                existing_files.add(obj.object_name)
        sleep(args.pause)


if __name__ == "__main__":
    main()

