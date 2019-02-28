# /bin/bash

# 配置job运行的shell脚本

prediction_day=1 # 质检数据时间
chat_filter_score=0.980 # chat质检分数阈值
speech_filter_score=0.600 #语音质检分数阈值

python_home=/home/q/python35/bin/python3.5 # python目录
main_func_home=/export/f_fuwu/fuwu_data_model/betac/model/qualitycheck #运行主函数所在目录

usage(){
    echo 'usage : -[d <prediction day>] -[c <chat filter score>] -[s <speech filter score>]'
}

while getopts ":d:c:s:" opt
do
    case $opt in
        d)
            prediction_day=$OPTARG
            ;;
        c)
            chat_filter_score=$OPTARG
            ;;
        s)
            speech_filter_score=$OPTARG
            ;;
        *)
            usage $0
            ;;
    esac
done

prediction_date=$(date +'%Y-%m-%d' -d "$prediction_day day ago")

# chat质检
$python_home $main_func_home/main.py --query_type "chat" --prediction_date $prediction_date --chat_filter_score $chat_filter_score &

# 语音质检
$python_home $main_func_home/main.py --query_type "speech" --prediction_date $prediction_date --speech_filter_score $speech_filter_score &

wait
# chat与语音质检以后进行数据整合
$python_home $main_func_home/integrated_data.py --prediction_date $prediction_date


