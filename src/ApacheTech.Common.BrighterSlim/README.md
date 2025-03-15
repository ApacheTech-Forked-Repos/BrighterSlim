# Brighter Slim

A massively slimmed down version of `Paramore.Brighter`, with no external dependencies. This is an experimental project, to add mediated command handling to the Gantry Mod Development Toolkit (MDK) for Vintage Story. This is not intended for public use, and has not been tested beyond the very basics required for proof of concept.

This project will not be updated very often, and will not keep up with the updates of `Paramore.Brighter`. I've ripped the guts out of the original repo, and stitched things back together so that it works, but this is not safe to be used for production purposes now. Even `Polly` has been stripped out of it, so there's no circuit breaking, no retry policies, and it requires a lot of plumbing within any consuming library to make it work.  
  
You can see an example within [`VintageStory.Gantry`](https://www.nuget.org/packages/VintageStory.Gantry/) as to how this has been implemented within the game.