"""===============================================================================

        FILE: /Users/nailbiter/Documents/forgithub/alex_leontiev_toolbox_python/alex_leontiev_toolbox_python/utils/project_management/network_diagrams.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2024-04-23T18:17:53.449622
    REVISION: ---

==============================================================================="""
import typing
import uuid
import logging
import more_itertools
from jinja2 import Template


class Activity:
    def __init__(self, duration: float, name: typing.Optional[str] = None):
        assert duration > 0
        self._name = str(uuid.uuid4()) if name is None else name
        self._duration = duration
        self._early_start = None
        self._early_finish = None
        self._late_start = None
        self._late_finish = None

    @property
    def name(self):
        return self._name

    @property
    def duration(self):
        return self._duration

    @property
    def early_start(self):
        return self._early_start

    def early_finish(self):
        return self._early_finish

    def late_start(self):
        return self._late_start

    def late_finish(self):
        return self._late_finish


class NetworkDiagram:
    def __init__(self, is_loud: bool = False):
        self._activities = {}
        self._dependencies = set()
        self._is_float_computed = False
        self._is_loud = is_loud
        self._logger = logging.getLogger(self.__class__.__name__)

    def add(self, activity: Activity, is_override: bool = False):
        assert is_override or activity.name not in self._activities
        self._activities[activity.name] = activity

    def depends_on(self, a: str, b: str):
        assert {a, b} <= set(self._activities)
        self._dependencies.add((a, b))

    def depends_chain(self, *l):
        for a, b in more_itertools.pairwise(l):
            self.depends_on(a, b)

    def compute_float(self, start: str, end: str):
        assert not self._is_float_computed
        self._is_float_computed = True

    def to_graphviz(self) -> str:
        """
        https://graphviz.org/doc/info/command.html
        """
        return Template(
            """
            digraph G{
            graph [
              rankdir = "LR"
            ];
            {% for node_name,node in activities|dictsort %}
            "{{ node_name }}" [
              label="{a|b|c\n}|{{ node_name }}\n|{d|e|f}"
              shape="record"
            ];
            {% endfor %}
            {% for a,b in dependencies %}
            "{{ a }}" -> "{{ b }}";
            {% endfor %}
            }
        """
        ).render(
            dict(activities=self._activities, dependencies=sorted(self._dependencies))
        )

    def update(self, l, **add_kwargs):
        for a in l:
            self.add(a, **add_kwargs)
