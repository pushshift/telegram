from telethon import functions, types


def message_media_from_json(message_json):
    media_type = message_json['media']['_']
    if media_type == 'MessageMediaDocument':
        return message_media_document_from_json(message_json)
    elif media_type == 'MessageMediaPhoto':
        return message_media_photo_from_json(message_json)


def message_media_document_from_json(json_object):
    # a document consists of 
    # 'access_hash' --> long
    #  'file_reference', --> bytes
    #  'date' --> date
    #  'mime_type'--> string
    #  'size' --> int
    #  'dc_id' --> int
    #  'attributes'

    media_json = json_object['media']
    document_json = media_json['document']
    attributes_json = document_json['attributes']
    thumbs_json = document_json['thumbs']
    
    # lets reconstruct attibutes first
    document_attributes = []
    attr_types_dict = {
        'DocumentAttributeAnimated': types.DocumentAttributeAnimated,
        'DocumentAttributeFilename': types.DocumentAttributeFilename,
        'DocumentAttributeImageSize': types.DocumentAttributeImageSize,
        'DocumentAttributeVideo': types.DocumentAttributeVideo,
        'DocumentAttributeAudio': types.DocumentAttributeAudio,
        'DocumentAttributeHasStickers': types.DocumentAttributeHasStickers,
        'DocumentAttributeSticker': types.DocumentAttributeSticker
        }

    for attr in attributes_json:
        attr_type = attr['_']
        del attr['_']
        document_attributes.append(attr_types_dict[attr_type](**attr))

    # reconstuct photo_sizes (thumbs)
    thumbs_object = photo_sizes_from_json(thumbs_json)

    del document_json['_']
    del document_json['attributes']
    del document_json['thumbs']
    document_json['attributes'] = document_attributes
    document_json['thumbs'] = thumbs_object

    return types.MessageMediaDocument(document=types.Document(**document_json))


def photo_sizes_from_json(list_of_sizes):
    photo_sizes = []
    for size in list_of_sizes:
        size_object_type = size['_']
        del size['_']
        if size_object_type == 'PhotoStrippedSize':
            photo_sizes.append(types.PhotoStrippedSize(**size))
        else:
            # reconstruct location first
            location_json = size['location']
            del location_json['_']
            location_object = types.FileLocationToBeDeprecated(**location_json)
            size['location'] = location_object
            if size_object_type == 'PhotoSize':
                photo_sizes.append(types.PhotoSize(**size)) 
            else:
                photo_sizes.append(types.PhotoCachedSize(**size))
    return photo_sizes


def message_media_photo_from_json(json_object):
    # a photo consists of 
    #id	long	
    #access_hash	long	
    #file_reference	bytes	
    #date	date	
    #sizes	PhotoSize	A list must be supplied.
    #dc_id	int	
    #has_stickers	flag	This argument defaults to None and can be omitted.

    media_json = json_object['media']
    photo_json =  media_json['photo']

    # fist lets reconstruct the sizes (PhotoSize onject)
    photo_sizes = photo_sizes_from_json(photo_json['sizes'])
    
    del photo_json['_']
    del photo_json['sizes']
    photo_json['sizes'] = photo_sizes
    photo_object = types.Photo(**photo_json)
    return types.MessageMediaPhoto(photo=photo_object)