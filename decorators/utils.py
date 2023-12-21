import sys
from typing import Optional, Callable


def decorate_function_by_name(decorator: Callable, function_path: str, module_path: Optional[str] = None):
    modules = sys.modules

    module_obj = modules['__main__']
    if module_path is not None:
        module_parts = module_path.split('.')

        root_module = module_parts.pop(0)
        if root_module in modules:
            search_module = modules[root_module]
        
            for part in module_parts:
                if not hasattr(search_module, part):
                    break

                search_module = getattr(search_module, part)
            else:
                module_obj = search_module

    func_parts = function_path.split('.')
    search_obj = module_obj

    func_name = func_parts.pop()
    for part in func_parts:
        search_obj = getattr(search_obj, part)
    try:
        orig_f = getattr(search_obj, func_name)

        setattr(search_obj, func_name, decorator(orig_f))
    except: pass