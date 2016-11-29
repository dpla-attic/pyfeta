#!/usr/bin/env bash

# Bootstrap an Amazon EMR cluster.
# ---
#
# See README.md.
#
# We install the specific version of Python that we want, and we install the
# Python packages that our spark_mapping and cdl_map modules require.  We
# install git because it is necessary for installing pyenv.
#

cd $HOME

echo "installing git"

sudo yum -y install git

if [ $? -ne 0 ]; then
    >&2 echo "failed to install git"
    exit 1
fi

echo "installing pyenv"

curl -L https://raw.githubusercontent.com/yyuu/pyenv-installer/master/bin/pyenv-installer | bash

if [ $? -ne 0 ]; then
    >&2 echo "failed to install pyenv"
    exit 1
fi

cat >> .bashrc <<END

export PYENV_ROOT="\$HOME/.pyenv"
export PATH="\$PYENV_ROOT/bin:\$PATH"
eval "\$(pyenv init -)"
END

export PYENV_ROOT="$HOME/.pyenv"
export PATH="$PYENV_ROOT/bin:$PATH"
eval "$(pyenv init -)"

echo "installing Python 3.5.2"

pyenv install 3.5.2 && pyenv global 3.5.2

if [ $? -ne 0 ]; then
    >&2 echo "failed to install Python"
    exit 1
fi

echo "installing Python packages"

for pkg in "boto3 rdflib"; do
    pip install $pkg
    if [ $? -ne 0 ]; then
        >&2 echo "failed to install $pkg"
        exit 1
    fi
done

echo "done"
