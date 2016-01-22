import unittest

from arbalest.redshift import S3CopyPipeline
from arbalest.contrib.luigi import PipelineTask
import six

if six.PY2:
    from mock import Mock, create_autospec
else:
    from unittest.mock import Mock, create_autospec


class PipelineTaskShould(unittest.TestCase):
    def test_run_given_pipeline(self):
        pipeline = create_autospec(S3CopyPipeline)
        pipeline.__name__ = 'S3CopyPipeline'
        pipeline.get = Mock()

        pipeline_task = PipelineTask(pipeline)
        pipeline_task.run()

        self.assertEqual(pipeline.get.called, True)
