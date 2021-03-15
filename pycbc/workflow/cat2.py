import os
import logging, copy
from ligo import segments
from pycbc.workflow.core import FileList, Executable, Node, File
from pycbc.workflow.segment import parse_cat_ini_opt

class PyCBCCalculateDqExecutable(Executable):
    # This is a wrapper around the pegasus class "Executable"
    # The arguments of pegasus.Executable can be found in
    # pycbc/workflow/core.py#L149
    # current_retention_level = Executable.ALL_TRIGGERS
    current_retention_level = Executable.MERGED_TRIGGERS

    def create_node(self, workflow, seg_file, cat2_file, flag):
        node = Node(self)
        # Here I have to add just the argument related to the workflow.
        # Everything else will have to be in analysis.ini
        # [calculate_dq]
        # Executable objects are initialized with ifo information
        node.add_opt('--ifo', self.ifo_string)
        node.add_opt('--flag', flag)
        node.add_input_opt('--science-segments', seg_file)
        node.add_input_opt('--cat2-segments', cat2_file)
        node.new_output_file_opt(workflow.analysis_time, '.hdf', '--output-file')
        # Comment: I can find these files using find_output_with_tag(self, tag)
        # from core.py
        return node


def create_cat2_timeseries(workflow, seg_file, cat2_file, cat2_name, option_name,
                           output_dir=None, tags=None):
    if not workflow.cp.has_option('workflow-coincidence', 'do-dq-fitting'):
        return FileList()
    # aggiungere elif se non c'e' nulla in segment-ifo cat2
    else:
        if not tags: tags=[]
        output = FileList()
        for ifo in workflow.ifos:
            flag_str = workflow.cp.get_opt_tags("workflow-segments", option_name, [ifo])
            flag_list = flag_str.split(',')
            for flag in flag_list:
                logging.info("Creating job for flag %s" %(flag))
                flag_tag = copy.copy(tags)
                flag_tag.append(flag[1:])
                logging.info("flag tag is %s" %(flag_tag))
                logging.info("tag is %s" %(tags))
                raw_exe = PyCBCCalculateDqExecutable(workflow.cp,
                                                     'calculate_dq', ifos=ifo,
                                                     out_dir=output_dir,
                                                     tags=flag_tag)
                raw_node = raw_exe.create_node(workflow, seg_file, cat2_file, 
                                               flag)
                workflow += raw_node
                output += raw_node.output_files
    return output
