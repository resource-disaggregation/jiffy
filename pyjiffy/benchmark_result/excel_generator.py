import xlrd
import xlwt


f = xlwt.Workbook() #创建工作簿

'''
创建第一个sheet:
    sheet1
'''
txt_name = ["file_bp.txt","file_bp_zipf.txt","file_cp.txt","file_cp_zipf.txt","file_cb.txt","file_cb_zipf.txt","ht_with_cache.txt","ht_zipf_with_cache.txt"]
numbers = "1234567890."
for i in range(len(txt_name)):
    try:
        file = open(txt_name[i])
        sheet_name = txt_name[i].rstrip(".txt")
        sheet = f.add_sheet(sheet_name,cell_overwrite_ok=True) #创建sheet
    except:
        continue
    
    if "bp" in txt_name[i]:
        row0 = ["5","10","15","20","25","30","35","40","45","50"]
        for i in range(len(row0)):
            sheet.write(0,i+1,row0[i])
        col_index = 1
        row_index = 1
        for line in file.readlines():
            line = line.lstrip(" =").rstrip(" =")
            if "Block Size=  " in line:
                block_size = ""
                index = len("Block Size=  ") + line.index("Block Size=  ")
                while line[index] in numbers:
                    block_size += line[index]
                    index += 1
                sheet.write(row_index,0,block_size)
            if "Prefetch Size=  " in line:
                prefetch_size = ""
                index = len("Prefetch Size=  ") + line.index("Prefetch Size=  ")
                while line[index] in numbers:
                    prefetch_size += line[index]
                    index += 1
                
            if "Hit_rate:  " in line:
                if prefetch_size == "5":
                    col_index = 1
                Hit_rate = ""
                index = len("Hit_rate:  ") + line.index("Hit_rate:  ")
                while line[index] in numbers:
                    Hit_rate += line[index]
                    index += 1
                sheet.write(row_index,col_index,str(float(Hit_rate)))
                col_index += 1
                
                if prefetch_size == "50":
                    row_index += 1
            

    elif "cb" in txt_name[i]:
        row0 = ["100","300","500","700","900","1100","1300","1500","1700","1900","2100"]
        for i in range(len(row0)):
            sheet.write(0,i+1,row0[i])
        col_index = 1
        row_index = 1
        for line in file.readlines():
            line = line.lstrip(" =").rstrip(" =")
            if "Cache_Size=  " in line:
                cache_size = ""
                index = len("Cache_Size=  ") + line.index("Cache_Size=  ")
                while line[index] in numbers:
                    cache_size += line[index]
                    index += 1
                
            if "Block Size=  " in line:
                block_size = ""
                index = len("Block Size=  ") + line.index("Block Size=  ")
                while line[index] in numbers:
                    block_size += line[index]
                    index += 1
                sheet.write(row_index,0,block_size)
            if "Hit_rate:  " in line:
                
                if block_size == "1000":
                    row_index = 1
                Hit_rate = ""
                index = len("Hit_rate:  ") + line.index("Hit_rate:  ")
                while line[index] in numbers:
                    Hit_rate += line[index]
                    index += 1
                sheet.write(row_index,col_index,str(float(Hit_rate)))
                row_index += 1
                if block_size == "20000":
                    
                    col_index += 1


    elif "cp" in txt_name[i]:
        row0 = ["5","10","15","20","25","30","35","40","45","50"]
        for i in range(len(row0)):
            sheet.write(0,i+1,row0[i])
        col_index = 1
        row_index = 1
        for line in file.readlines():
            line = line.lstrip(" =").rstrip(" =")
            if "Cache_Size=  " in line:
                cache_size = ""
                index = len("Cache_Size=  ") + line.index("Cache_Size=  ")
                while line[index] in numbers:
                    cache_size += line[index]
                    index += 1
                sheet.write(row_index,0,cache_size)
            if "Prefetch Size=  " in line:
                prefetch_size = ""
                index = len("Prefetch Size=  ") + line.index("Prefetch Size=  ")
                while line[index] in numbers:
                    prefetch_size += line[index]
                    index += 1
            if "Hit_rate:  " in line:
                if prefetch_size == "5":
                    col_index = 1
                Hit_rate = ""
                index = len("Hit_rate:  ") + line.index("Hit_rate:  ")
                while line[index] in numbers:
                    Hit_rate += line[index]
                    index += 1
                sheet.write(row_index,col_index,str(float(Hit_rate)))
                col_index += 1
                if prefetch_size == "50":
                    row_index += 1
        
            
    else:
        row0 = ["cache_size","throughput","hit_rate"]
        for i in range(len(row0)):
            sheet.write(0,i,row0[i])
        count = 1
        for line in file.readlines():
            line = line.lstrip(" =").rstrip(" =")
            if "Cache_Size=  " in line:
                Cache_size = ""
                index = len("Cache_Size=  ") + line.index("Cache_Size=  ")
                while line[index] in numbers:
                    Cache_size += line[index]
                    index += 1
                sheet.write(count,0,Cache_size)
            if "Throughput:  " in line:
                throughput = ""
                index = len("Throughput:  ") + line.index("Throughput:  ")
                while line[index] in numbers:
                    throughput += line[index]
                    index += 1
                sheet.write(count,1,str(float(throughput)))
            if "Hit_rate:  " in line:
                hit_rate = ""
                index = len("Hit_rate:  ") + line.index("Hit_rate:  ")
                while line[index] in numbers:
                    hit_rate += line[index]
                    index += 1
                sheet.write(count,2,hit_rate)
                count += 1
   

f.save(r'final_hr.xls')