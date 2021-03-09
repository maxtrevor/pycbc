import os
import logging
from ligo import segments
from pycbc.workflow.core import FileList, Executable, Node, File


class PyCBCCalculateDqExecutable(Executable):
    # This is a wrapper around the pegasus class "Executable"
    # The arguments of pegasus.Executable can be found in
    # pycbc/workflow/core.py#L149
    current_retention_level = Executable.ALL_TRIGGERS
    def create_node(self, cat2_file, flag, ifo, start, end):
        node = Node(self)
        # Here I have to add just the argument related to the workflow.
        # Everything else will have to be in analysis.ini 
        # [calculate_dq]

        # Executable objects are initialized with ifo information
        segment = segments.segment(start, end)
        node.add_opt('--ifo', self.ifo_string)
        node.add_input_opt('--cat2-segments', cat2_file)
        node.add_opt('--gps-start-time', start)
        node.add_opt('--gps-end-time', end)
        node.add_opt('--flag', flag)
        node.new_output_file_opt(segment, '.hdf', '--output-file')        
        return node


def create_cat2_timeseries(workflow, cat2_file, cat2_name, option_name, output_dir=None, tags=None):   
    if not option_name in workflow.cp.get_subsections('workflow-segments'):
        return FileList()
    else:
        start = workflow.analysis_time[0]
        end = workflow.analysis_time[1]
        
        output = FileList()
        for ifo in workflow.ifos:
            flag_str = workflow.cp.get_opt_tags("workflow-segments", option_name, [ifo])
            # I have to declare the executable in executable.ini with something like 
            # calculate_dq = pycbc_calculate_dq. I have to do this for each exec.

            # Probably it will read letter by letter. 
            for flag in flag_str:
                print(flag_str)
                raw_exe = PyCBCCalculateDqExecutable(workflow.cp, 
                                                     'calculate_dq', ifos=ifo,
                                                     out_dir=output_dir, 
                                                     tags=tags)
                # How does it know what name to give to the output file??
                raw_node = raw_exe.create_node(cat2_files, flag_str, ifo, start, end)
                workflow += raw_node
                output += raw_node.output_files
        return output
