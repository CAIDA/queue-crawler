from typing import Optional
import asyncio

class AsyncQueueCall:
    def __init__(self, call_id:str, f, uuid:Optional[str]=None):
        self.id = call_id
        self.f = f
        self.uuid = uuid

    def __repr__(self) -> str:
        repr_str = self.id
        if self.uuid:
            repr_str += f"({self.uuid})"
        return f"`{repr_str}`"

class AsyncQueue:
    def __init__(self):
        self.past_calls = {}
        self.active_calls = {}

    def is_done(self, call_id) -> bool:
        return call_id in self.past_calls

    def is_active(self, call_id) -> bool:
        return call_id in self.active_calls

    def is_new(self, call_id) -> bool:
        return not (self.is_done(call_id) or self.is_active(call_id))

    async def queue_call(self, call:AsyncQueueCall):
        if self.is_active(call.id):
            # If a call has already been made, wait for it to finish
            await self.active_calls[call.id].wait()
        elif not self.is_done(call.id):
            # If the call has not been made before, start it and cache 
            # the final response
            call_event = asyncio.Event()
            self.active_calls[call.id] = call_event
            res = await call.f;
            self.past_calls[call.id] = res
            self.active_calls.pop(call.id)
            call_event.set()
        # Close any unused coroutines
        call.f.close()
        return self.past_calls[call.id]