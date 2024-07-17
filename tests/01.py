from tezcanalyticx.events import EventManager,EventX,Period
import unittest as UT


class TezcanalyticXTest(UT.IsolatedAsyncioTestCase):
    async def test_example(self):
        em = EventManager()
        e = EventX(event_type="PUT", bucket_id="b1",key = "k1",size=10,response_time = 2.5,replicas= ["p1","p2"])
        await em.add_event(event=e)
        print(em.periods)