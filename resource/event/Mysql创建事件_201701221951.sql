CREATE EVENT even_main
 ON SCHEDULE EVERY 20 MINUTE 
 DO CALL proc_main();
 
 任务状(0 --> 未发送; 1 --> 已发送,未更新邮件表,日志表状态, 2 -->已发送,已更新邮件表,日志表状态(完成); -1 --> 发送失败)