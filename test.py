import json
file_path = "/Users/apple/BerTopic/data/snapshots/twitter_20260330_113520.json"
write_path = "/Users/apple/BerTopic/data/snapshots/twitter_texts_20260330_113520.txt"
with open(file_path, "r", encoding='utf-8') as f_r, \
    open(write_path, "w", encoding="utf-8") as f_w:
        data = json.load(f_r)
        texts = data["texts"]
        
        RT_cnt = 0
        RT_omit_cnt = 0
        omit_cnt = 0
        total_cnt = 0
        for text in texts:
            if text["content_text"].startswith("RT @"):
                RT_cnt += 1
                if text["content_text"].endswith("…") or text["content_text"].endswith("..."):
                     RT_omit_cnt += 1
            
            if text["content_text"].endswith("…") or text["content_text"].endswith("..."):
                 omit_cnt += 1
            total_cnt += 1
            f_w.write(text["content_text"])
            f_w.write("\n")
        
        f_w.write("\n")
        f_w.write(f"Total counts: {total_cnt} \n")
        f_w.write(f"RT_cnt: {RT_cnt}\n")
        f_w.write(f"RT_omit_cnt:{RT_omit_cnt} \n")
        
        print(f"Total counts: {total_cnt}")
        print(f"Omit_cnt: {omit_cnt}")
        print(f"RT_cnt: {RT_cnt}")
        print(f"RT_omit_cnt:{RT_omit_cnt} ")
        print(f"NonRT_cnt: {total_cnt - RT_cnt}")
        print(f"Correct_cnt: {total_cnt - omit_cnt}")

        
