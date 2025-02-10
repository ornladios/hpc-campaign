#!/usr/bin/env python3
import nacl.encoding
import nacl.secret
import nacl.utils
import nacl.pwhash
import uuid
import yaml
from datetime import datetime, UTC
from getpass import getpass
from os.path import exists

class Key:
    """ Key file for encryption of campaign metadata """

    def __init__(self):
        self.id = None
        self.password_encrypted = False
        self.key = None
        self.date = datetime.now(UTC)
        self.note = ""
        self.salt = None

    def generate(self, note: str, pwd: bytes=None):
        self.date = datetime.now(UTC)
        self.note = note

        # Generate a secret key
        unencrypted_key = nacl.utils.random(nacl.secret.SecretBox.KEY_SIZE)

        # Generate a UUID for the key
        self.id = uuid.uuid4().hex

        if pwd:
            kdf = nacl.pwhash.argon2i.kdf
            self.salt = nacl.utils.random(nacl.pwhash.argon2i.SALTBYTES)
            pkey = kdf(nacl.secret.SecretBox.KEY_SIZE, pwd, self.salt,
                    opslimit=nacl.pwhash.argon2i.OPSLIMIT_SENSITIVE, 
                    memlimit=nacl.pwhash.argon2i.MEMLIMIT_SENSITIVE)
            pbox = nacl.secret.SecretBox(pkey)
            pnonce = nacl.utils.random(nacl.secret.SecretBox.NONCE_SIZE)
            self.key = pbox.encrypt(unencrypted_key, pnonce)
        else:
            self.key = unencrypted_key
            self.salt = None

    def generate_interactive(self, password_required: bool=False):
        print("Type a note for this key: ", end='')
        note = input()
        if password_required:
            while True:
                pwd = bytes(getpass("Create password: "), 'utf-8')
                pwdcheck = bytes(getpass("Retype password: "), 'utf-8')
                if pwd != pwdcheck:
                    print ("Password mismatch. Try again.")
                else:
                    break
        else:
            pwd = None
        self.generate(note, pwd)

    def write(self, path: str):
        doc = {}
        doc['note'] = self.note
        doc['id'] = self.id
        doc['key'] = self.key.hex()
        if self.salt:
            doc['salt'] = self.salt.hex()
        doc['date'] = self.date.isoformat()
        with open(path, 'w') as file:
            yaml.dump(doc, file, sort_keys=False)

    def read(self, path: str):
        with open(path, 'r') as file:
            doc = yaml.safe_load(file)

        self.note = doc['note']
        self.id = doc['id']
        self.password_encrypted = False
        self.key = bytes.fromhex(doc['key'])
        self.date = doc['date']
        
        if 'salt' in doc:
            self.salt = bytes.fromhex(doc['salt'])
        else:
            self.salt = None


    def get_decrypted_key(self, pwd: bytes=None) -> bytes:
        if self.salt:
            if not pwd:
                pwd = bytes(getpass("Password: "), 'utf-8')
            kdf = nacl.pwhash.argon2i.kdf
            pkey = kdf(nacl.secret.SecretBox.KEY_SIZE, pwd, self.salt,
                            opslimit=nacl.pwhash.argon2i.OPSLIMIT_SENSITIVE, 
                            memlimit=nacl.pwhash.argon2i.MEMLIMIT_SENSITIVE)
            pbox = nacl.secret.SecretBox(pkey)
            return pbox.decrypt(self.key)
        else:
            return self.key

    
    def info(self, do_verify: bool=False) -> bool:
        print(f"created on: {datetime.fromisoformat(self.date)}")
        print(f"      note: {self.note}")
        print(f"      uuid: {self.id}")
        if self.salt:
            print(f"      encryption: password")
        else:
            print(f"      encryption: none")

        if do_verify and self.salt:
            try:
                key = self.get_decrypted_key()
            except:
                print("Password is incorrect")
                return False
            print("Password is correct")
        return True


def read_key(path: str) -> Key:
    if not exists(path):
        print(f"Could not find key file {path}")
        exit(1)
    key = Key()
    key.read(path)
    return key
