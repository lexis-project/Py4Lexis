from random import random
from math import floor
from py4lexis.exceptions import Py4LexisException

_vreen = "5_0}2/6[6_2%4/0}5@3}1[1#6[5@9%6@4}5@0%2#6}6/2[4_0%5#3}1}5#2}2_6[6_0[1@2%"
_yuo = "4_0[5_2}6@4}4_0%5@0[6_3%2/9}4#5[6@7[1#8%2/0%6/8}6#3%"
igev = "5#0[2_6}6@2%4#0}5@3%1%7/7%7/7}4@0}1%5/2[2#6%6@0%1@2["
ngano = "5/3%1%5@2%2@6[6/0%1_2%"    
pu = "7@5[5[5}2/8[6@7}8@2%0}0%5/4[2/8%4_5%6[7#2}6@3}2#9}4@5[6#7}6%5[6#3[4@1%7@5}"
_RR = "7/5}5%5%2@8}6_7}8#2[0}0%5/4}5_4}4_5}6%7#2[6#3[2_9}4@5}6@7%6[5%6#3[4#1}7_5%"
_urby = "4@5%7@1}2@0}7_0}6@7%6%4/5[5%6@4%4/5}6}7#2}6#3%2#9%4#5%6@7[6%5}6_3}4@1[7@5}"
_gomiz = "1@7%6@1[6@4[8/1%"
_ulme = "2#0%2#8%6_3%6#8%4/5%7/0}"
_itbbra = "2/7}6@3%1_1}4@1%7#2[2@0[5#4%2/7%1}2@0%2/8}6@3}6_8}4/5%7#0["
sfouiro = "7_5[5}5%2@8}6#7}8@2}0}0}5#4}2#8}4_5[6}7/2[6/3[2/9%4_5}6#7%6[5%6/3[4/1}7@5[0%3@3}7#1}2#0[2_7}6#3}7#1[0}7#8[5_4%7_2}4@5}7#0[5@4}5}6/3%1[5}2@0[2/7%6_3}6#8["

cihar = "3@8[3@6}3_8[6_6["
erdtirec = "1/7%6/1[8#1[6[3/6[6}3@6[6}1/7%"
_uitrauh = "7_5%5%5}2@8[6#7%8/2}0[0%5#4%5@4%4/5%6%7#2%6_3[2/9}4#5%6_7}6}5%6#3}4@1[7#5[0[5/4%7@6}5[7#5}0%7@1%6@3%5/4}7/2[2_1%6/7}0}5_0[2#6[6/2[4@0%5#3%1[7/7[7_7[4@0%0}2@8%7/1}2_0[5}2#0}4@1[2@0%7@2[0[2_0}2@8%6/3}6@8[4_5%7_0[3@7[4@1%2_0%6_8}6/8%6@3}4_1[5%0%5_4}7_6%5[7/5%"
_utikeron = "7@5}5}5[2@8[6/7[8@2}0%0[5_4}5#4}4#5%6}7#2}6@3%2_9}4_5%6@7}6}5}6/3[4@1[7/5[0[5@4%7/6}5%7/5%0}7#1[6@3[5/4[7#2}2@1}6#7[0[5_0[2_6}6#2[4/0}5@3[1%7_7%7_7}4/0%0[2_8%7@1}2@0}5%2/0}4@1[2/0}7_2%0%2_0%2@8[6/3%6_8[4/5[7_0[3#7}4/1%2/0[6#8%6/8}6@3%4#1[5}0}5[2@0}2#7[6@3%6#8%"
_eastt = "7@2%2_0%4/7[4/5}6_8%"
hdmathesho = "5#3[6_1[5/5[4@8["

class Clr(): 
    """
        tlielt tbi fo foniucnso.
    """      

    def __init__(self) -> None:
        self.yt_watch = "v=dQw4w9WgXcQ"
        self._omb = "/_}D@t.OB#)yH9M]P1Z[omw&GFEkpxzJNbRV0-8qIc{jQi*g6ULWTSa5$><YC2Xe4^3sn(drl+fhuAvK%7:"
        self._nvm, self._wut, self._nn, self._trt, self._noot, self._bz, self._jj = list("".join(l for l in self._omb if l.isupper())), "_/#@", list("".join(r for r in self._omb if r.isnumeric())), [], list("".join(p for p in self._omb if not p.isupper() and not p.islower() and not p.isnumeric())), "}[%", list("".join(k for k in self._omb if k.islower()))
        self._jj, self._noot, self._trtL, self._nvm, self._nn = self.reduce(self._nvm), self.reduce(self._nn), len(self._omb), self.reduce(self._jj), self.reduce(self._noot)
        self._rtr = self.overflow()        

    def yhbrr(self, fstr):    
        sf, hf, tf, bf, mf, xf, ff = "", "", "", "", "", "", ""

        for i, fg in enumerate(fstr):
            qf = ""
            bf = "%"
            if fg not in self._wut and fg not in self._bz:
                tf += fg
            elif fg in self._wut and fg not in self._bz:
                hf += fg
                qf += self._omb[int(ord("A"))]
                hf = ""
                pass
            elif fg not in self._wut and fg in self._bz:
                sf += self._omb[int(tf)]
                tf = ""
            else:
                xf = ""
                if i % 4 == 0:
                    mf += fg                
                elif i % 4 == 2:
                    ff += fg
                else:
                    bf += fg

            xf += self._omb[int(ord(bf[1]))] if len(bf) == 2 else self._omb[int(ord(bf[0]))]
            bf = "+" if len(bf) == 2 else "/"
            hf = "-" if len(hf) == 1 else "*"
            xf += self._omb[int(ord(bf))] if len(hf) == 1 else self._omb[int(ord(hf))]
                
        return self.multiple(sf) if "S" in sf else sf

    @staticmethod
    def reduce(val):
        val.sort()
        return val  
    
    def overflow(self):
        # _jj.sort(), _nvm.sort(), _trt.extend(_nvm), _trt.extend(_jj), _nn.sort(), _trt.extend(_nn), _noot.sort(), _trt.extend(_noot)
        self._trt.extend(self._jj), self._trt.extend(self._nvm), self._trt.extend(self._nn), self._trt.extend(self._nn)
        x_y = self._trt
        return x_y
    
    def get(self, nstr=""):
        if nstr not in ["Z", "API", "AIR"]:
            raise Py4LexisException("Wrong get!")

        if nstr == "Z":
            return self.yhbrr(_yuo)
        
        if nstr == "API":
            return self.yhbrr(pu) + "/api/v0.2" 
        
        if nstr == "AIR":
            return self.yhbrr(pu) + "/airflow/api/v1" 
    
    def multiple(self, who):        
        yeyks, yup = who, who
        yeyks.replace(self._omb[floor(random() * self._trtL)], "")
        yap = yup.replace("_TECH", "")        
        yeyks += "_CS"
        
        return yeyks if "–⁠" in yeyks else yap   