create PROCEDURE main_hive2mysql_send_email()
BEGIN
    # 每20分钟检测一次
	# select SLEEP(1200)
	SELECT COUNT(1) INTO @moblie_name_num
	FROM data_market.idl_moblie_name_ft
	WHERE
    `status` = 0 AND mobile_city IS NULL AND mobile_operators IS NULL;
    
    # mobile_province,total_num,name_num_avg,max_relation_avg,data_date
	IF (@moblie_name_num>0) THEN 
		INSERT INTO email_service.config_email_info (conf_id,email_content,send_status,send_num,create_date,update_date)
            SELECT
            10 AS conf_id,
            group_concat(
                CONCAT_WS('__&__',moblie_name.mobile_province,moblie_name.total_num,moblie_name.name_num_avg,moblie_name.max_relation_avg,moblie_name.data_date)
                SEPARATOR "__$__"
            ) AS email_content,
            0 AS send_status,0 AS send_num,now() AS create_date,now() AS update_date
            FROM
                (
                    SELECT
                    IF (mobile_province IS NULL,'all',mobile_province) as mobile_province,
                    total_num,name_num_avg,max_relation_avg,data_date
                FROM
                    data_market.idl_moblie_name_ft
                WHERE
                    `status` = 0 AND mobile_city IS NULL AND mobile_operators IS NULL
                ) moblie_name;
        
        update data_market.idl_moblie_name_ft set status = 1;
	END IF;

	SELECT COUNT(1) INTO @moblie_name_num
	FROM data_market.idl_limao_order_ft
	WHERE
    `status` = 0;
    
	IF (@moblie_name_num>0) THEN 
		INSERT INTO email_service.config_email_info (conf_id,email_content,send_status,send_num,create_date,update_date)
            SELECT
            11 AS conf_id,
            group_concat(
                CONCAT_WS('__&__', total_num, new_num, data_date)
                SEPARATOR "__$__"
            ) AS email_content,
            0 AS send_status,0 AS send_num,now() AS create_date,now() AS update_date
            FROM
                data_market.idl_limao_order_ft
            WHERE
                `status` = 0;
        update data_market.idl_limao_order_ft set status = 1;
	END IF;

	SELECT COUNT(1) INTO @receiver_num
	FROM data_market.ald_limao_receiver_ft
	WHERE
    `status` = 0;
    
	SELECT COUNT(1) INTO @recommended_cid_num
	FROM data_market.adl_limao_recommended_cid_ft
	WHERE
    `status` = 0 
		and suritem_name is null;
    
	IF (@recommended_cid_num>0) THEN 
		INSERT INTO email_service.config_email_info (conf_id,email_content,send_status,send_num,create_date,update_date)
			SELECT
			12 AS conf_id,
			group_concat(
					CONCAT_WS('__&__', info.item_name, info.mobile_num, info.cobj_avg, info.data_date)
					SEPARATOR "__$__"
			) AS email_content,
			0 AS send_status,0 AS send_num,now() AS create_date,now() AS update_date
			FROM
					(select if(item_name is null, 'all', item_name) as item_name, mobile_num, cobj_avg, data_date 
						from data_market.adl_limao_recommended_cid_ft  
						where `status` = 0 and suritem_name is null 
						ORDER BY mobile_num desc limit 20) info;
        update data_market.adl_limao_recommended_cid_ft set status = 1;
	END IF;

	IF (@receiver_num>0) THEN 
		INSERT INTO email_service.config_email_info (conf_id,email_content,send_status,send_num,create_date,update_date)
            SELECT
            13 AS conf_id,
            group_concat(
                CONCAT_WS('__&__', total_num, weigth_avg, receiver_avg, data_date)
                SEPARATOR "__$__"
            ) AS email_content,
            0 AS send_status,0 AS send_num,now() AS create_date,now() AS update_date
            FROM
                data_market.ald_limao_receiver_ft
            WHERE
                `status` = 0; 
        update data_market.ald_limao_receiver_ft set status = 1;
	END IF;
    
	SELECT COUNT(1) INTO @info_num
	FROM data_market.idl_limao_info_ft
	WHERE
    `status` = 0;
    
	IF (@info_num>0) THEN 
		INSERT INTO email_service.config_email_info (conf_id,email_content,send_status,send_num,create_date,update_date)
            SELECT
            14 AS conf_id,
            group_concat(
                CONCAT_WS('__&__', email_type, email_num, data_date)
                SEPARATOR "__$__"
            ) AS email_content,
            0 AS send_status, 0 AS send_num, now() AS create_date,now() AS update_date
            FROM
                (select email_type, email_num, data_date from data_market.idl_limao_info_ft where `status` = 0 ORDER BY email_num DESC limit 20) info;

        update data_market.idl_limao_info_ft set `status` = 1;
	END IF;
END