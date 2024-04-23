"""===============================================================================

        FILE: /Users/nailbiter/Documents/forgithub/alex_leontiev_toolbox_python/tests/test___float_computation.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2024-04-23T18:18:40.484615
    REVISION: ---

==============================================================================="""
import logging
from alex_leontiev_toolbox_python.utils.project_management.network_diagrams import (
    Activity,
    NetworkDiagram,
)
import json


def test_float_computation():
    nd = NetworkDiagram()
    nd.add(Activity(3, "A"))
    nd.add(Activity(6, "B"))
    nd.add(Activity(2, "E"))
    nd.add(Activity(3, "H"))
    nd.add(Activity(3, "J"))
    nd.add(Activity(2, "K"))
    nd.add(Activity(7, "C"))
    nd.add(Activity(4, "F"))
    nd.add(Activity(9, "D"))
    nd.add(Activity(1, "G"))
    nd.add(Activity(6, "I"))

    nd.depends_chain("A", "B", "E", "H", "J", "K")
    nd.depends_chain(*list("ACFHJK"))
    nd.depends_chain(*list("ADGIK"))

    nd.set_start("A")
    nd.set_end("K")

    nd.compute_float()

    with open("/tmp/g1.dot", "w") as f:
        f.write(nd.to_graphviz(is_highlight_critical_path=True))

    with open("/tmp/paths.txt", "w") as f:
        json.dump(nd.get_paths("A", "K"), f)
