import * as Avatar from '@radix-ui/react-avatar'

export function PersonAvatar({
  name,
  url,
  group = false,
  large = false,
}: {
  name: string
  url?: string
  group?: boolean
  large?: boolean
}) {
  return (
    <Avatar.Root
      className={`inline-flex shrink-0 items-center justify-center overflow-hidden rounded-2xl font-semibold ${large ? 'size-12 text-lg' : 'size-10 text-sm'} ${group ? 'bg-emerald-50 text-emerald-600' : 'bg-blue-50 text-accent'}`}
    >
      {url && /^https?:\/\//i.test(url) && (
        <Avatar.Image
          src={url}
          alt=""
          className="size-full object-cover"
          referrerPolicy="no-referrer"
        />
      )}
      <Avatar.Fallback delayMs={url ? 200 : 0}>
        {group ? '群' : Array.from(name)[0]?.toUpperCase() || '?'}
      </Avatar.Fallback>
    </Avatar.Root>
  )
}
