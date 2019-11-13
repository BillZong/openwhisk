#!/usr/bin/env expect

if {$argc < 5} {
send_user "usage: $argv0 host port user ssh_key_file_path join_token_cmd_file"
exit
}

# 超时时间
set timeout 5

set host [lindex $argv 0]
set port [lindex $argv 1]
set user [lindex $argv 2]
set ssh_key_file_path [lindex $argv 3]

# proc getJoinTokenCmd {cmdFile Key {Comment "#"} {Equal "="}} { ;#过程中如果参数有缺省值，使用花括号引起，并赋值   
# 读取文件
proc getJoinTokenCmd {cmdFile} {
    puts "cmdFile: $cmdFile"
    set cmd ""
    
    set err [catch {set fileid [open $cmdFile r]} errMsg]
    if {$err == 1} {
        puts "errMsg : $errMsg"   
        return $cmd 
    }
    # 成功打开文件后, 读取内容
    while {[gets $fileid line] != -1} {
        set cmd $line
    }

    close $fileid
    #返回值  
    return $cmd
}
set join_token_cmd [getJoinTokenCmd [lindex $argv 4]]

#spawn表示开启新expect会话进程
spawn ssh -i $ssh_key_file_path -p $port $user@$host

# 检测密钥方式连接和密码，没有会超时自动跳过
expect "yes/no" { send "no\r"; exp_continue }

expect "$user@"

send "which kubeadm; echo RET=$?\r"
expect "RET=0"

set timeout 5

send "$join_token_cmd; echo RET=$?\r"
expect "RET=0"

send "exit 0\r"
#expect eof表示结束expect和相应进程会话结束，如果用interact会保持停留在当前进程会话
expect eof
