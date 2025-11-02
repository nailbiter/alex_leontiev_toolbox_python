# --- Examples ---

from alex_leontiev_toolbox_python.utils.simple_eval import safe_eval_expression

# A safe dictionary of variables
my_vars = {"x": 10, "y": 5, "is_admin": False}


def test():
    # Safe: Simple arithmetic
    # print(f"'x + (y * 2)' => {}")
    assert safe_eval_expression("x + (y * 2)", my_vars) == 20

    # Safe: Boolean logic
    # print(f"'x > 5 and not is_admin' => {safe_eval_expression('x > 5 and not is_admin', my_vars)}")
    assert safe_eval_expression("x > 5 and not is_admin", my_vars) == True

    assert safe_eval_expression("7/2", my_vars) == 3.5

    # # Safe: Unary operator
    # print(f"'-x' => {safe_eval_expression('-x', my_vars)}")

    # print("\n### Unsafe/Invalid Expressions ###")
    # # Unsafe: Function call
    # print(f"'print(x)' => {safe_eval_expression('print(x)', my_vars)}")

    # # Unsafe: Attribute access
    # print(f"'x.__class__' => {safe_eval_expression('x.__class__', my_vars)}")

    # # Unsafe: Import
    # print(f"'__import__(\"os\")' => {safe_eval_expression('__import__(\"os\")', my_vars)}")

    # # Invalid: Undefined variable
    # print(f"'x + z' => {safe_eval_expression('x + z', my_vars)}")
