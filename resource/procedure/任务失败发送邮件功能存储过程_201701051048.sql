create procedure if not exist proc_create_sending_email_task(in email_id int, in cur_date varchar(20))
begin
    insert into running_email_info
    select 
        null as run_id,
        conf.conf_id as conf_id,
        email_id as email_id,
        email.attachment_name as attachment_name,
        conf.temple_path as temple_path,
        email.picture_name as picture_name,
        conf.email_theme,
        email.email_content as email_content_convert,
        conf.email_content_type as email_content_type,
        conf.email_content_encode as email_content_encode,
        sender.sender_email as sender_email,
        sender.sender_name as sender_name,
        sender.sender_passwd as sender_passwd,
        sender.email_server as email_server,
        sender.server_port as server_port,
        receiver.receiver_list as receiver_list,
        0 as run_status,
        cur_date as create_date,
        cur_date as update_date
    from 
        config_email_conf conf, 
        config_email_info email, 
        (select conf_id, GROUP_CONCAT(receiver_email) as  receiver_list from config_receiver_info group by conf_id) receiver, 
        config_sender_info sender
    where 
        email.email_id = email_id
        and email.conf_id = conf.conf_id 
        and conf.conf_id = receiver.conf_id 
        and conf.sender_id = sender.sender_id;
end

create function func_create_sending_email_task(email_id int, cur_date varchar(20))
returns int
begin
    insert into running_email_info
    select 
        null as run_id,
        conf.conf_id as conf_id,
        email_id as email_id,
        email.attachment_name as attachment_name,
        conf.temple_path as temple_path,
        email.picture_name as picture_name,
        conf.email_theme,
        email.email_content as email_content_convert,
        conf.email_content_type as email_content_type,
        conf.email_content_encode as email_content_encode,
        sender.sender_email as sender_email,
        sender.sender_name as sender_name,
        sender.sender_passwd as sender_passwd,
        sender.email_server as email_server,
        sender.server_port as server_port,
        receiver.receiver_list as receiver_list,
        0 as run_status,
        cur_date as create_date,
        cur_date as update_date
    from 
        config_email_conf conf, 
        config_email_info email, 
        (select conf_id, GROUP_CONCAT(receiver_email) as  receiver_list from config_receiver_info group by conf_id) receiver, 
        config_sender_info sender
    where 
        email.email_id = email_id
        and email.conf_id = conf.conf_id 
        and conf.conf_id = receiver.conf_id 
        and conf.sender_id = sender.sender_id;
        
    return (select max(run_id) from running_email_info);
end

create procedure if not exist proc_main
BEGIN
    set @now_time= date_add(now(),INTERVAL 8 hour);
    declare email_id int;
    set email_id = func_check_email_status(0);
    select email_id
    
END

create function if not exist func_check_email_status(send_status int)
returns int
BEGIN
	#Routine body goes here...
	RETURN (select email_id from config_email_info email where email.send_status = send_status limit 1);
END

create procedure proc_email_update(in run_id int , in cur_date varchar(20))
BEGIN

    update 
        config_email_info email, 
        running_email_info run 
    set 
        email.send_status = run.run_status, 
        email.send_num = (email.send_num + 1), 
        email.update_date = cur_date 
    where 
        run.run_id = run_id
        and email.email_id = run.email_id;
END

create procedure proc_logging_email_insert(in email_id int , in cur_date varchar(20), in log_status int)
BEGIN

    insert into logging_email_log
    select 
        null as log_id,
        email.conf_id as conf_id,
        email.email_id as email_id,
        log_status as log_status,
        cur_date as create_date
    from 
        config_email_info email 
    where 
        email.email_id = email_id;
END