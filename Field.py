BeginString = 8
BodyLength = 9
MsgType = 35
SenderCompID = 49
TargetCompID = 56
TargetSubID = 57
SenderSubID = 50
MsgSeqNum = 34
SendingTime = 52
CheckSum = 10
TestReqID = 112
EncryptMethod = 98
HeartBtInt = 108
ResetSeqNum = 141
Username = 553
Password = 554
Text = 58
BeginSeqNo = 7
EndSeqNo = 16
GapFillFlag = 123
NewSeqNo = 36
MDReqID = 262
SubscriptionRequestType = 263
MarketDepth = 264
MDUpdateType = 265
NoMDEntryTypes = 267
NoMDEntries = 268
MDEntryType = 269
NoRelatedSym = 146
Symbol = 55
MDEntryPx = 270
MDUpdateAction = 279
MDEntryID = 278
MDEntrySize = 271


class Groups:
    MDEntry_Snapshot = (MDEntryType, MDEntryPx)
    MDEntry_Refresh = (
        MDUpdateAction,
        MDEntryType,
        MDEntryID,
        Symbol,
        MDEntryPx,
        MDEntrySize
    )
