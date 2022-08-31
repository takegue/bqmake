declare labels array<struct<key string, value string>>;
call `fn.update_table_labels_partition`((null, 'zpreview_test'));
