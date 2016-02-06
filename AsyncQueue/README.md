Example usage:

Clients needs to provide
* Specify # workers
* Function (status, args => FuncResult)
* Can submit anytime, can query result

Sample

```python

def addNumber(status, a, b, *args, **kwargs):
    
    status.setStatus('Installing Part A')
    time.sleep(3)
    status.setStatus('Installing Part B')
    time.sleep(3)
    status.setStatus('Combining Part A and B')
    time.sleep(3)
    status.setStatus('Calculating')
    time.sleep(3)

    result = a + b
    error  = None
    
    return FuncResult(result=result, error=error)


buf = createBufferWithWorker(6)

reqs = []
for i in xrange(10, 31):
    job = Job(addNumber, (15, i), {})
    reqs.append(buf.put(job, 100))

# END request
buf.put(None, 100)

```

Request IDs in <strong>reqs</strong> can be used to get progress and result.



