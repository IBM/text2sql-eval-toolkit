#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

class BasePipeline:
    def run_pipeline(self, input_data):
        raise NotImplementedError("Subclasses should implement this method.")

    def get_results(self):
        raise NotImplementedError("Subclasses should implement this method.")