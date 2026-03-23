from typing import Optional, Mapping, List
import pytest
import snakemake.common.tests
from snakemake_interface_executor_plugins.settings import ExecutorSettingsBase
from snakemake_interface_common.plugin_registry.plugin import TaggedSettings
from snakemake_executor_plugin_htcondor import ExecutorSettings


# Check out the base classes found here for all possible options and methods:
# https://github.com/snakemake/snakemake/blob/main/src/snakemake/common/tests/__init__.py
@pytest.mark.integration
class TestWorkflowsBase(snakemake.common.tests.TestWorkflowsBase):
    __test__ = True

    # override the get_envvars() to include "PATH"
    def get_envvars(self) -> List[str]:
        return ["PATH"]

    def get_executor(self) -> str:
        return "htcondor"

    def get_executor_settings(self) -> Optional[ExecutorSettingsBase]:
        # instantiate ExecutorSettings of this plugin as appropriate
        return ExecutorSettings()

    def get_default_storage_prefix(self) -> Optional[str]:
        return None

    def get_default_storage_provider(self) -> Optional[str]:
        return None

    def get_default_storage_provider_settings(
        self,
    ) -> Optional[Mapping[str, TaggedSettings]]:
        return None
