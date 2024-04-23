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
import operator
import functools


class Activity:
    def __init__(self, duration: float, name: typing.Optional[str] = None):
        assert duration >= 0
        self._name = str(uuid.uuid4()) if name is None else name
        self._duration = duration
        self._early_start = None
        self._early_finish = None
        self._late_start = None
        self._late_finish = None

    def _increment_early(self, early_start):
        self._early_start = early_start
        self._early_finish = early_start + self._duration - 1

    def _decrement_late(self, late_finish):
        self._late_finish = late_finish
        self._late_start = late_finish - self._duration + 1

    @property
    def name(self):
        return self._name

    @property
    def duration(self):
        return self._duration

    @property
    def float(self):
        return (
            None
            if (self._early_start is None or self._late_start is None)
            else self._late_start - self._early_start
        )

    @property
    def early_start(self):
        return self._early_start

    @property
    def early_finish(self):
        return self._early_finish

    @property
    def late_start(self):
        return self._late_start

    @property
    def late_finish(self):
        return self._late_finish


class NetworkDiagram:
    def __init__(self, is_loud: bool = False):
        self._activities = {}
        self._dependencies = set()
        self._is_float_computed = False
        self._is_loud = is_loud
        self._logger = logging.getLogger(self.__class__.__name__)
        self._start = None
        self._end = None

    def add(self, activity: Activity, is_override: bool = False):
        assert is_override or activity.name not in self._activities
        self._activities[activity.name] = activity

    def depends_on(self, a: str, b: str):
        assert {a, b} <= set(self._activities)
        self._dependencies.add((a, b))

    def depends_chain(self, *l):
        for a, b in more_itertools.pairwise(l):
            self.depends_on(a, b)

    def _forward_path(self, start: str, end: str):
        covered = {
            start,
        }
        self._activities[start]._increment_early(1)
        while covered < set(self._activities):
            for x in sorted(set(self._activities) - covered):
                previous_activities = self.get_previous_activities(x)
                if set(previous_activities) <= covered:
                    early_start = max(
                        [self._activities[y].early_finish for y in previous_activities]
                    )
                    self._activities[x]._increment_early(early_start + 1)
                    covered.add(x)

    def _backwark_path(self, start: str, end: str):
        covered = {
            end,
        }
        self._activities[end]._decrement_late(self._activities[end].early_finish)
        while covered < set(self._activities):
            for x in sorted(set(self._activities) - covered):
                next_activities = self.get_next_activities(x)
                if set(next_activities) <= covered:
                    late_finish = min(
                        [self._activities[y].late_start for y in next_activities]
                    )
                    assert late_finish is not None, (x, next_activities)
                    self._activities[x]._decrement_late(late_finish - 1)
                    covered.add(x)

    def compute_float(self):
        assert not self._is_float_computed
        assert self._start is not None and self._end is not None
        start, end = self._start, self._end

        self._forward_path(start, end)
        self._backwark_path(start, end)

        self._is_float_computed = True

    def set_start(self, start: str) -> None:
        assert start in self._activities
        self._start = start

    def set_end(self, end: str) -> None:
        assert end in self._activities
        self._end = end

    @functools.cache
    def get_paths(self, start: str, end: str) -> list[typing.Tuple[list[str], float]]:
        assert {start, end} <= set(self._activities)
        res = []
        if start == end:
            return [
                (start, self._activities[start].duration),
            ]
        else:
            next_ones = self.get_next_activities(start)
            for next_one in next_ones:
                res.extend(
                    [
                        (
                            [start, *path],
                            path_duration + self._activities[start].duration,
                        )
                        for path, path_duration in self.get_paths(next_one, end)
                    ]
                )
            return res

    @functools.cache
    def get_next_activities(self, node: str) -> list[str]:
        return [k for k in self._activities if (node, k) in self._dependencies]

    @functools.cache
    def get_previous_activities(self, node: str) -> list[str]:
        return [k for k in self._activities if (k, node) in self._dependencies]

    def to_graphviz(self, is_highlight_critical_path: bool = False) -> str:
        """
        https://graphviz.org/doc/info/command.html
        """
        if is_highlight_critical_path:
            assert self._start is not None and self._end is not None
            start, end = self._start, self._end
            paths = self.get_paths(self._start, self._end)
            self._logger.warning(paths)
            critical_path, _ = max(paths, key=lambda path: (path[1], tuple(path[0])))
            self._logger.warning(critical_path)
        else:
            critical_path = []

        return Template(
            """
            digraph G{
            graph [
              rankdir = "LR"
            ];
            {% for node_name, a in activities|dictsort %}
            "{{ node_name }}" [
              label="{{"{"}}{{ impute(a.early_start) }}|{{ a.duration }}|{{ impute(a.early_finish) }}\n{{"}"}}|{{ node_name }}\n|{{"{"}}{{ impute(a.late_start) }}|{{ impute(a.float) }}|{{ impute(a.late_finish) }}{{"}"}}"
              shape="record"
              {% if node_name in critical_path %}
              fillcolor=yellow
              style=filled
              {% endif %}
            ];
            {% endfor %}
            {% for a,b in dependencies %}
            "{{ a }}" -> "{{ b }}";
            {% endfor %}
            }
        """
        ).render(
            dict(
                activities=self._activities,
                dependencies=sorted(self._dependencies),
                impute=lambda x: "" if x is None else x,
                critical_path=critical_path,
            )
        )

    def update(self, l, **add_kwargs):
        for a in l:
            self.add(a, **add_kwargs)
