#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

set -x

PYTHON_MAJOR_MINOR=$(python3 -V | sed -E 's/.* ([[:digit:]]+)\.([[:digit:]]+).*/\1\2/')
WHEEL_FILE=$(ls /pulsar/pulsar-client | grep "cp${PYTHON_MAJOR_MINOR}")
pip3 install /pulsar/pulsar-client/${WHEEL_FILE}[all]

# The following `update build-essential python3-dev` are added due to grpcio `cc` not found error
# TODO: remove these lines if the error got resolved.
# WARNING: currently grpcio build takes long.
apt-get update
apt-get -y install build-essential python3-dev
