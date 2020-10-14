import os
import logging
from ligo import segments
from pycbc.workflow.core import FileList, Executable, Node, File

class PyCBCCalculateiDQExecutable(Executable):
    current_retention_level = Executable.MERGED_TRIGGERS
    def create_node(self, segment):
        start = int(segment[0])
        end = int(segment[1])
        node = Node(self)
        node.add_input_opt('--gps-start-time', start)
        node.add_input_opt('--gps-end-time', end)
        node.new_output_file_opt(segment, '.hdf', '--output-file')        
        return node

class PyCBCRerankiDQExecutable(Executable):    
    current_retention_level = Executable.MERGED_TRIGGERS
    def create_node(self, ifo, idq_files):
        node = Node(self)
        node.add_input_opt('--ifo', ifo)
        node.add_input_list_opt('--input-file', idq_files)
        node.new_output_file_opt(idq_files[0].segment, '.hdf', '--output-file')        
        return node
    
def setup_iqd_reranking(workflow, segs, output_dir=None, tags=None):
    if not workflow.cp.has_option('workflow-coincidence', 'do-trigger-fitting'):
        return FileList()
    else:
        output = FileList()
        for ifo in workflow.ifos:
            idq_files = FileList()
            for seg in segs:
                raw_exe  = PyCBCCalculateiDQExecutable(workflow.cp,
                                                   'calculate_idq', ifos=ifo,
                                                   out_dir=output_dir,
                                                   tags=tags)
                raw_node = raw_exe.create_node(seg)
                workflow += raw_node
                idq_files += raw_node.output_files
            
            new_exe = PyCBCRerankiDQExecutable(workflow.cp,
                                                   'rerank_idq', ifos=ifo,
                                                   out_dir=output_dir,
                                                   tags=tags)
            new_node = new_exe.create_node(ifo, idq_files)
            workflow += new_node
            output += new_node.output_files
               
        return output