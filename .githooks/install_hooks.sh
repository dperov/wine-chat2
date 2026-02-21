#!/bin/sh

HOOKS="pre-commit post-checkout post-merge cryptor.sh"

for hook in $HOOKS; do
    if [ -f "./.githooks/$hook" ]; then
        cp "./.githooks/$hook" ".git/hooks/$hook"
        chmod +x ".git/hooks/$hook"
        echo "Установлен $hook"
    else
        echo "!! Внимание: ./.githooks/$hook не найден, пропущено"
    fi
done

echo "Готово!"
