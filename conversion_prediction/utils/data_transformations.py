def unique_list(list_to_simplify):
    list_to_simplify = set(list_to_simplify)
    list_to_simplify = list(list_to_simplify)
    list_to_simplify = [element for element in list_to_simplify if len(element) > 0]

    return list_to_simplify
