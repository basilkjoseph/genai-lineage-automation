import env_params as ep

tables =[
]


def env_params_mapper(tables):
    final_tbl_list=[]
    for tbl in tables:
        unmapped_schema,tbl_name=tbl.split(".")
        unmapped_sch = unmapped_schema.replace("${", "").replace("{", "").replace("}", "")
        try:
            mapped_sch=ep.env_params[unmapped_sch]
        except:
            mapped_sch=unmapped_schema
        final_tbl_name=mapped_sch+"."+tbl_name
        final_tbl_list.append(final_tbl_name)
        print(final_tbl_name)
    return final_tbl_list


env_params_mapper(tables)
