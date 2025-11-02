"""===============================================================================

        FILE: /Users/nailbiter/Documents/forgithub/alex_leontiev_toolbox_python/alex_leontiev_toolbox_python/utils/simple_eval.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: generated with Gemini https://gemini.google.com/share/8058cc291085
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2025-11-02T14:18:59.163033
    REVISION: ---

==============================================================================="""
import ast
import operator
import typing

# 1. Define your "whitelist" of allowed operations.
# This is the most critical step.

# Allow basic operators
ALLOWED_OPERATORS = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.Pow: operator.pow,
    ast.USub: operator.neg,
    ast.Not: operator.not_,
}

# Allow boolean logic
ALLOWED_BOOLEAN_OPS = {
    ast.And: lambda a, b: a and b,
    ast.Or: lambda a, b: a or b,
}

# Allow comparisons
ALLOWED_COMPARATORS = {
    ast.Eq: operator.eq,
    ast.NotEq: operator.ne,
    ast.Lt: operator.lt,
    ast.LtE: operator.le,
    ast.Gt: operator.gt,
    ast.GtE: operator.ge,
}

# We can also define a whitelist of *node types* for an extra layer of safety.
# This list is more permissive than the one in the code below,
# but shows what you'd typically allow for simple arithmetic.
ALLOWED_NODE_TYPES = {
    "Expression",  # The overall container
    "BinOp",  # Binary operations (e.g., 1 + 2)
    "BoolOp",  # Boolean operations (e.g., x and y)
    "Compare",  # Comparisons (e.g., x > 5)
    "UnaryOp",  # Unary operations (e.g., -1)
    "Name",  # Variable names (e.g., x)
    "Constant",  # Literals (e.g., 5, True, "hello")
    "Load",  # Context for loading a variable
    # --- Anything not in this list is implicitly BANNED ---
    # Banned includes: Call, Import, Attribute, Subscript, etc.
}


# 2. Create the recursive evaluator
def _safe_eval_node(node, variables):
    """Recursively evaluate a single AST node."""

    node_type = type(node)

    if node_type is ast.Constant:
        # This is a simple literal (number, string, True, False)
        return node.value

    elif node_type is ast.Name:
        # This is a variable. Look it up in our safe context.
        if node.id in variables:
            return variables[node.id]
        else:
            raise NameError(f"Variable '{node.id}' is not defined.")

    elif node_type is ast.BinOp:
        # Binary operation: (left_node op right_node)
        if type(node.op) not in ALLOWED_OPERATORS:
            raise TypeError(f"Operation {type(node.op).__name__} is not allowed.")

        left_val = _safe_eval_node(node.left, variables)
        right_val = _safe_eval_node(node.right, variables)
        op_func = ALLOWED_OPERATORS[type(node.op)]
        return op_func(left_val, right_val)

    elif node_type is ast.UnaryOp:
        # Unary operation: (op operand) e.g., -1
        if type(node.op) not in ALLOWED_OPERATORS:
            raise TypeError(f"Operation {type(node.op).__name__} is not allowed.")

        operand_val = _safe_eval_node(node.operand, variables)
        op_func = ALLOWED_OPERATORS[type(node.op)]
        return op_func(operand_val)

    elif node_type is ast.BoolOp:
        # Boolean operation: (x and y)
        if type(node.op) not in ALLOWED_BOOLEAN_OPS:
            raise TypeError(f"Operation {type(node.op).__name__} is not allowed.")

        op_func = ALLOWED_BOOLEAN_OPS[type(node.op)]
        # Note: 'and' and 'or' are short-circuiting, but this
        # implementation will evaluate all operands.
        # A true short-circuiting eval is more complex.
        values = [_safe_eval_node(v, variables) for v in node.values]

        # Simple fold for 'and'/'or'
        if type(node.op) is ast.And:
            result = True
            for v in values:
                if not v:
                    result = False
                    break
            return result
        elif type(node.op) is ast.Or:
            result = False
            for v in values:
                if v:
                    result = True
                    break
            return result

    elif node_type is ast.Compare:
        # Comparison: (left op1 right1 op2 right2...)
        # We'll only support simple, single comparisons like (x > 5)
        if len(node.ops) != 1 or len(node.comparators) != 1:
            raise SyntaxError("Only single comparisons are allowed (e.g., x > 5).")

        op_type = type(node.ops[0])
        if op_type not in ALLOWED_COMPARATORS:
            raise TypeError(f"Comparison {op_type.__name__} is not allowed.")

        left_val = _safe_eval_node(node.left, variables)
        right_val = _safe_eval_node(node.comparators[0], variables)
        op_func = ALLOWED_COMPARATORS[op_type]
        return op_func(left_val, right_val)

    # 3. If we find any node we don't recognize, raise an error.
    else:
        raise TypeError(f"Disallowed node type: {node_type.__name__}")


def safe_eval_expression(
    expression_string: str, variables: dict = {}
) -> typing.Union[float, int, bool]:
    """
    WARNING: This is a simplified example.
        Safely evaluates a simple arithmetic/boolean expression.

        Args:
            expression_string (str): The string to evaluate.
            variables (dict): A dictionary of allowed variable names and their values.
    """
    try:
        # Parse the string. mode='eval' means it expects a single expression.
        tree = ast.parse(expression_string, mode="eval")

        # The tree root is an 'Expression' node. We need to evaluate its 'body'.
        if not isinstance(tree, ast.Expression):
            raise SyntaxError("Not a valid expression.")

        # Start the recursive evaluation
        return _safe_eval_node(tree.body, variables)

    except (SyntaxError, TypeError, NameError, KeyError, ZeroDivisionError) as e:
        # Handle parsing or evaluation errors
        return f"Error: {e}"
    except Exception as e:
        # Catch any other unexpected errors
        return f"An unexpected error occurred: {e}"
