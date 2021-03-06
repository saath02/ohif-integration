#!/usr/bin/env python

# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

try:
    # `conftest` cannot be imported when running in `nox`, but we still
    # try to import it for the autocomplete when writing the tests.
    from conftest import Utils
except ModuleNotFoundError:
    Utils = None
import pytest

NAME = "dataflow/custom-containers/minimal"


@pytest.fixture(scope="session")
def bucket_name(utils: Utils) -> str:
    yield from utils.storage_bucket(NAME)


@pytest.fixture(scope="session")
def container_image(utils: Utils) -> str:
    yield from utils.cloud_build_submit(NAME)


@pytest.fixture(scope="session")
def run_dataflow_job(utils: Utils, bucket_name: str, container_image: str) -> str:
    yield from utils.cloud_build_submit(
        config="run.yaml",
        substitutions={
            "_IMAGE": container_image,
            "_JOB_NAME": utils.hyphen_name(NAME),
            "_TEMP_LOCATION": f"gs://{bucket_name}/temp",
            "_REGION": utils.region,
        },
        source="--no-source",
    )


def test_custom_container_minimal(utils: Utils, run_dataflow_job: str) -> None:
    # Wait until the job finishes.
    job_id = utils.dataflow_job_id(utils.hyphen_name(NAME))
    utils.dataflow_jobs_wait(job_id)
