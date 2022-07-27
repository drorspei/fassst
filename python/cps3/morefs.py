import fsspec
from fsspec.spec import AbstractFileSystem
import os
import re

listdir_template = (
    r'<tr><td valign="top"><img src="[^"]*" alt="[^"]*"></td><td>'
    r'<a href="([^"]*)">[^<]*</a></td><td align="right">[^<]*</td>'
    r'<td align="right">([^<]*)</td>'
)

class HttpFtpFS(AbstractFileSystem):
    protocol = "httpftp"
    
    def __init__(self, real_protocol="https", **kwargs):
        self.real_protocol = real_protocol
    
    def listdir(self, path):
        with fsspec.open(
            path.replace("httpftp://", f"{self.real_protocol}://", 1), 'r'
        ) as f:
            a = f.read()
        ret =  [
            {
                **{
                    "Key": os.path.join(path, key),
                    "type": "directory" if s == "  - " else "file"
                },
                **(
                    {}
                    if s.strip() == "-"
                    else {
                        "size": (
                            float(s.strip()[:-1])
                            * (
                                1024 * (s[-1] == "K")
                                + 1024 * 1024 * (s[-1] == "M")
                            )
                        )
                    }
                )
            }
            for key, s in re.findall(listdir_template, a)

        ]
        return ret
    
    def open(self, path, mode="rb"):
        assert mode == "rb" or mode == "r"
        return fsspec.open(
            path.replace("httpftp://", f"{self.real_protocol}://", 1),
            mode=mode
        ).open()

fsspec.register_implementation("httpftp", HttpFtpFS)
