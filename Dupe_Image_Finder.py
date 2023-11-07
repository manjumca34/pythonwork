import pathlib
import glob
import sqlite3
import subprocess


class ImageData:
    def __init__(self, dir):
        self.rootdir = dir
        self.con = sqlite3.connect('test.db')
        self.cur = self.con.cursor()
        self.cur.execute("delete from imagedata")

    def scan_images(self):
        for filename in glob.iglob(self.rootdir + '**/**/*.pdf', recursive=True):
            result = subprocess.run(['cksum', filename], stdout=subprocess.PIPE)
            cksum = int(result.stdout.decode('ascii').split(' ')[0])
            fpath = result.stdout.decode('ascii').split(' ')[2]
            fpath = fpath.replace('\n', '')
            dirlevels = fpath.count("/")
            fname = pathlib.Path(fpath).name
            insrt_qry = f"INSERT INTO imagedata(fullpath,imagename,checksum,dirlevels,parent,mark_for_del,deleted) VALUES('{fpath}','{fname}','{cksum}',{dirlevels},'N','N','N')"
            print(insrt_qry)
            self.cur.execute(insrt_qry)

    def mark_for_del(self):
        self.cur.execute("""UPDATE IMAGEDATA 
                        SET mark_for_del='Y'
                        WHERE not exists (select 1 from 
                                    (SELECT fullpath,checksum  FROM 
		                                    (SELECT im.fullpath,
				                                    im.checksum,
				                                    ROW_NUMBER() OVER(PARTITION BY im.checksum ORDER BY im.fullpath ASC) as  rownum
				                            FROM IMAGEDATA im
				                            INNER JOIN 
				                            (SELECT checksum as parntcsum,
						                        max(dirlevels) as maxdirlevel 
						                        FROM imagedata group by checksum)a
				                            ON im.checksum=a.parntcsum
				                            AND  im.dirlevels=a.maxdirlevel
		                                    )inr 
                                            WHERE inr.rownum=1
                                    ) SELREC 
                        WHERE  IMAGEDATA.fullpath=SELREC.fullpath)""")

    def delImages(self):
        res1 = self.cur.execute("""select fullpath,imagename,checksum,parent,mark_for_del from imagedata where mark_for_del='Y' and deleted='N' """)
        res = res1.fetchall()
        print("in delImages")
        for record in res:
            delfile = record[0]
            delresult = subprocess.run(['rm', '-f', delfile], stdout=subprocess.PIPE)
            print(f"deleted {delfile}")
            upd_query = f"UPDATE imagedata SET deleted='Y' WHERE fullpath='{delfile}';"
            print(upd_query)
            updres = self.cur.execute(upd_query)

    def printdata(self,mdind,delind):
        sel_qry=f"SELECT * FROM IMAGEDATA where mark_for_del='{mdind}' and deleted='{delind}'"
        sel=self.cur.execute(sel_qry)
        for rec in sel :
            print(rec)





if __name__ == '__main__':
    imageproc=ImageData('/pass/the/parent folder/path for images ')
    imageproc.scan_images()
    imageproc.printdata('N','N')
    imageproc.mark_for_del()
    imageproc.printdata('Y','N')
    imageproc.delImages()
    imageproc.printdata('Y','Y')

