#!/bin/sh

# usage: ./cryptor.sh encrypt <plaintext> <ciphertext> <password_file>
#     or: ./cryptor.sh decrypt <ciphertext> <plaintext> <password_file>

ACTION="$1"
SRC="$2"
DST="$3"
PASS="$4"

if [ "$ACTION" = "encrypt" ]; then
    if [ ! -f "$SRC" ]; then
        echo "[cryptor] Файл для шифровки $SRC не найден!"
        exit 1
    fi
    if [ ! -f "$PASS" ]; then
        echo "[cryptor] Парольный файл $PASS не найден!"
        exit 1
    fi
    openssl enc -aes-256-cbc -salt -in "$SRC" -out "$DST" -pass file:"$PASS"
    if [ $? -eq 0 ]; then
        echo "[cryptor] $SRC зашифрован в $DST"
        exit 0
    else
        echo "[cryptor] Ошибка шифрования!"
        exit 2
    fi
elif [ "$ACTION" = "decrypt" ]; then
    if [ ! -f "$SRC" ]; then
        echo "[cryptor] Файл для расшифровки $SRC не найден!"
        exit 1
    fi
    if [ ! -f "$PASS" ]; then
        echo "[cryptor] Парольный файл $PASS не найден!"
        exit 1
    fi
    openssl enc -d -aes-256-cbc -in "$SRC" -out "$DST" -pass file:"$PASS" 2>/dev/null
    if [ $? -eq 0 ]; then
        echo "[cryptor] $SRC расшифрован в $DST"
        exit 0
    else
        echo "[cryptor] Ошибка расшифровки!"
        exit 2
    fi
else
    echo "Использование:"
    echo "$0 encrypt <plaintext> <ciphertext> <password_file>"
    echo "$0 decrypt <ciphertext> <plaintext> <password_file>"
    exit 3
fi