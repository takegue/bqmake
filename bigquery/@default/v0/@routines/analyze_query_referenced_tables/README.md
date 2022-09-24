Get referenced tables by query

  Arguments:
  ===
    ret: output variable to store the referenced tables
    query: query to analyize
    optiosn:
      enable_query_rewrite: Enable dynamic query rewrite to minimize slot or billing scan amount (default: false)
      default_region: region-us (default: "region-us")
  