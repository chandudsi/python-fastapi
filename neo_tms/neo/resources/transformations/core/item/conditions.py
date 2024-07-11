def is_itemPackage_level_update(*args):
    itemgroupId_enrichment = args[0][0]
    itemgroupId_without_enrichment = args[0][1]
    is_3PL_enabled = args[0][2]
    create_flag = args[0][3]
    if not create_flag:
        if is_3PL_enabled:
            if itemgroupId_enrichment == itemgroupId_without_enrichment:
                return True
            else:
                return False
        else:
            return True
    else:
        return False