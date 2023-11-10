import os
import shutil
import hashlib

def calcHash(filename):
    """
    Calculate the MD5 for a file
    """
    md5_hash = hashlib.md5()
    with open(filename, "rb") as f:
        # Read and update hash in chunks of 4K
        for byte_block in iter(lambda: f.read(4096), b""):
            md5_hash.update(byte_block)
        return str(md5_hash.hexdigest()).upper()


def main():
    # Read from source, write to dest using HASH as filename
    for root, dirs, files in os.walk("extracted"):
        for name in files:
            filename=os.path.join(root, name)
            print(filename)
            md5 = calcHash(filename)
            if not os.path.exists(f'binaries/{md5}'):
                shutil.copyfile(filename,f'binaries/{md5}')
        for name in dirs:
            print(os.path.join(root, name))

if __name__ == "__main__":
    main()
