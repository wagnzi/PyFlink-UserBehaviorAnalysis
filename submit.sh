flink run -pyExecutable /opt/anaconda3/envs/flink/bin/python3 \
  -python hot_items.py \
  -pyfs base_window.py,constants.py,output_tag.py

flink run -pyExecutable /opt/anaconda3/envs/flink/bin/python3 \
  -python UvWithBloom.py \
  -pyfs base_window.py
